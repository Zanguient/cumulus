/**
 * This module overrides the Kes Class and the Lambda class of Kes
 * to support specific needs of the Cumulus Deployment.
 *
 * Specifically, this module changes the default Kes Deployment in the following ways:
 *
 * - Adds the ability to add Cumulus Configuration for each Step Function Task
 *    - @fixCumulusMessageSyntax
 *    - @extractCumulusConfigFromSF
 * - Generates a public and private key to encrypt private information
 *    - @generateKeyPair
 *    - @uploadKeyPair
 *    - @crypto
 * - Creates Cumulus Message Templates for each Step Function Workflow
 *    - @template
 *    - @generateTemplates
 * - Adds Cumulus Message Adapter code to any Lambda Function that uses it
 * - Uploads the public/private keys and the templates to S3
 * - Restart Existing ECS tasks after each deployment
 * - Redeploy API Gateway endpoints after Each Deployment
 *
 */

'use strict';


const zipObject = require('lodash.zipobject');
const { Kes, utils } = require('kes');
const fs = require('fs-extra');
const Handlebars = require('handlebars');

const path = require('path');
const util = require('util');
const Lambda = require('./lambda');
const { crypto } = require('./crypto');
const { fetchMessageAdapter } = require('./adapter');
const { extractCumulusConfigFromSF, generateTemplates } = require('./message');

const fsWriteFile = util.promisify(fs.writeFile);


/**
 * Makes setTimeout return a promise
 *
 * @param {integer} ms - number of milliseconds
 * @returns {Promise} the arguments passed after the timeout
 */
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A subclass of Kes class that overrides opsStack method.
 * The subclass checks whether the public/private keys are generated
 * and uploaded to the deployment bucket. If not, they are generated and
 * uploaded.
 *
 * After the successful deployment of a CloudFormation template, the subclass
 * generates and uploads payload and StepFunction templates and restarts ECS
 * tasks if there is an active cluster with running tasks.
 *
 * @class UpdatedKes
 */
class UpdatedKes extends Kes {
  /**
   * Overrides the default constructor. It updates the default
   * Lambda class and adds a git repository path for the cumulus
   * message adapter
   *
   * @param {Object} config - kes config object
   */
  constructor(config) {
    super(config);
    this.Lambda = Lambda;
    this.messageAdapterGitPath = `${config.repo_owner}/${config.message_adapter_repo}`;
  }


  /**
   * Redeploy the given api gateway (more info: https://docs.aws.amazon.com/apigateway/latest/developerguide/how-to-deploy-api.html)
   *
   * @param {string} name - the name of the api gateway deployment (used for logging)
   * @param {string} restApiId - the api gateway id
   * @param {string} stageName - the deployment stage name
   * @returns {Promise.<boolean>} returns true if successful
   */
  async redeployApiGateWay(name, restApiId, stageName) {
    const waitTime = 20;
    if (restApiId) {
      try {
        const apigateway = new this.AWS.APIGateway();
        await apigateway.createDeployment({ restApiId, stageName }).promise();
        console.log(`${name} endpoints with the id ${restApiId} redeployed.`);
      }
      catch (e) {
        if (e.message && e.message.includes('Too Many Requests')) {
          console.log(
            `Redeploying ${restApiId} was throttled. `
            + `Another attempt will be made in ${waitTime} seconds`
          );
          await delay(waitTime * 1000);
          return this.redeployApiGateWay(name, restApiId, stageName);
        }
        throw e;
      }
    }
    return true;
  }

  /**
   * Restart all active tasks in the clusters of a deployed
   * CloudFormation
   *
   * @param  {Object} config - Kes Config object
   * @returns {Promise} undefined
   */
  async restartECSTasks(config) {
    const ecs = new this.AWS.ECS();

    // only restart the tasks if the user has turned it on the config
    if (config.ecs.restartTasksOnDeploy) {
      try {
        let resources = [];
        const params = { StackName: config.stackName };
        while (true) { // eslint-disable-line no-constant-condition
          // eslint-disable-next-line no-await-in-loop
          const data = await this.cf.listStackResources(params).promise();
          resources = resources.concat(data.StackResourceSummaries);
          if (data.NextToken) params.NextToken = data.NextToken;

          else break;
        }

        const clusters = resources.filter((item) => {
          if (item.ResourceType === 'AWS::ECS::Cluster') return true;
          return false;
        });

        for (let clusterCtr = 0; clusterCtr < clusters.length; clusterCtr += 1) {
          const cluster = clusters[clusterCtr];
          // eslint-disable-next-line no-await-in-loop
          const tasks = await ecs.listTasks({ cluster: cluster.PhysicalResourceId }).promise();

          for (let taskCtr = 0; taskCtr < tasks.length; taskCtr += 1) {
            const task = tasks.taskArns[taskCtr];
            console.log(`restarting ECS task ${task}`);
            // eslint-disable-next-line no-await-in-loop
            await ecs.stopTask({
              task: task,
              cluster: cluster.PhysicalResourceId
            }).promise();
            console.log(`ECS task ${task} restarted`);
          }
        }
      }
      catch (err) {
        console.log(err);
      }
    }
  }

  /**
   * Override CF parse to add Handlebars template helpers
   *
   * @param  {string} cfFile - Filename
   * @returns {string}        - Contents of cfFile templated using Handlebars
   */
  parseCF(cfFile) {
    Handlebars.registerHelper(
      'ifEquals',
      (arg1, arg2, options) =>
        ((arg1 === arg2) ? options.fn(this) : options.inverse(this))
    );

    Handlebars.registerHelper(
      'ifNotEquals',
      (arg1, arg2, options) =>
        ((arg1 !== arg2) ? options.fn(this) : options.inverse(this))
    );

    return super.parseCF(cfFile);
  }

  /**
   * Override CF compilation to inject cumulus message adapter
   *
   * @returns {Promise} returns the promise of an AWS response object
   */
  compileCF() {
    const filename = this.config.message_adapter_filename || '';
    const customCompile = this.config.customCompilation || '';
    const kesBuildFolder = path.join(this.config.kesFolder, 'build');
    const unzipFolderName = path.basename(filename, '.zip');

    const src = path.join(process.cwd(), kesBuildFolder, filename);
    const dest = path.join(process.cwd(), kesBuildFolder, 'adapter', unzipFolderName);

    // If custom compile configuration flag not set, skip custom compilation
    if (!customCompile) return super.compileCF();

    // If not using message adapter, don't fetch it
    if (!filename) return this.superCompileCF();

    return fetchMessageAdapter(
      this.config.message_adapter_version,
      this.messageAdapterGitPath,
      filename,
      src,
      dest
    ).then(() => {
      this.Lambda.messageAdapterZipFileHash = new this.Lambda(this.config).getHash(src);
      return this.superCompileCF();
    });
  }


  /**
   * setParentConfigvalues - Overrides nested stack template with parent values
   * defined in the override_with_parent config key
   */
  setParentOverrideConfigValues() {
    if (!this.config.parent) return;
    const parent = this.config.parent;
    this.config.override_with_parent.forEach((value) => {
      this.config[value] = parent[value] || this.config[value];
    });
  }

  /**
   * Modified version of Kes superclass compileCF method
   *
   * Compiles a CloudFormation template in Yaml format.
   *
   * Reads the configuration yaml from `.kes/config.yml`.
   *
   * Writes the template to `.kes/cloudformation.yml`.
   *
   * Uses `.kes/cloudformation.template.yml` as the base template
   * for generating the final CF template.
   *
   * @returns {Promise} returns the promise of an AWS response object
   */
  async superCompileCF() {
    this.setParentOverrideConfigValues();
    const lambda = new this.Lambda(this.config);
    if (this.config.lambdaProcess) {
      this.config = await lambda.process();
    }
    else {
      lambda.buildAllLambdaConfiguration('lambdas');
    }

    let cf;

    // Inject Lambda Alias values into configuration,
    // then update configured workflow lambda references
    // to reference the generated alias values

    if (this.config.useWorkflowLambdaVersions === true) {
      if (this.config.oldLambdaInjection === true) {
        lambda.buildAllLambdaConfiguration('workflowLambdas');
        await this.injectOldWorkflowLambdaAliases();
      }
      if (this.config.injectWorkflowLambdaAliases === true) {
        this.injectWorkflowLambdaAliases();
      }
    }

    // Update workflowLambdas with generated hash values
    lambda.addWorkflowLambdaHashes();

    // if there is a template parse CF there first
    if (this.config.template) {
      const mainCF = this.parseCF(this.config.template.cfFile);

      // check if there is a CF over
      try {
        fs.lstatSync(this.config.cfFile);
        const overrideCF = this.parseCF(this.config.cfFile);

        // merge the the two
        cf = utils.mergeYamls(mainCF, overrideCF);
      }
      catch (e) {
        if (!e.message.includes('ENOENT')) {
          console.log(`compiling the override template at ${this.config.cfFile} failed:`);
          throw e;
        }
        cf = mainCF;
      }
    }
    else {
      cf = this.parseCF(this.config.cfFile);
    }
    const destPath = path.join(this.config.kesFolder, this.cf_template_name);

    console.log(`Template saved to ${destPath}`);
    return fsWriteFile(destPath, cf);
  }


  /**
   *
   * @param {Object} lambda - AWS lambda object
   * @param {Object} config - AWS listAliases configuration object.
   * @returns {Promise.Object[]} returns the promise of an array of AWS Alias objects
   */
  async getAllLambdaAliases(lambda, config) {
    const lambdaConfig = Object.assign({}, config);
    let aliasPage;
    try {
      aliasPage = await lambda.listAliases(lambdaConfig).promise();
    }
    catch (err) {
      if (err.statusCode === 404) {
        return [];
      }
      throw (err);
    }

    if (!aliasPage.NextMarker) {
      return aliasPage.Aliases;
    }
    const aliases = aliasPage.Aliases;
    lambdaConfig.Marker = aliasPage.NextMarker;

    return aliases.concat(await this.getAllLambdaAliases(lambda, lambdaConfig));
  }

  /**
   * Using the object configuration, this function gets the 'config.maxNumerOfRetainedLambdas'
   * number of most recent lambda alias names to retain in the 'Old Lambda Resources' section of
   * the LambdaVersion template, avoiding duplicates of items in the Current Lambda section.
   *
   * @returns {Promise.string[]} returns the promise of a list of alias metadata
   *          objects: keys (Name, humanReadableIdentifier)
   **/
  async getRetainedLambdaAliasMetadata() {
    const awsLambda = new this.AWS.Lambda();
    const cumulusAliasDescription = 'Cumulus AutoGenerated Alias';
    const configLambdas = this.config.workflowLambdas;
    const numberOfRetainedLambdas = this.config.maxNumberOfRetainedLambdas;

    let aliasMetadataObjects = [];

    const lambdaNames = Object.keys(configLambdas);
    const aliasListsPromises = lambdaNames.map(async (lambdaName) => {
      const listAliasesConfig = {
        MaxItems: 10000,
        FunctionName: `${this.config.stackName}-${lambdaName}`
      };
      return this.getAllLambdaAliases(awsLambda, listAliasesConfig);
    });

    const aliasLists = await Promise.all(aliasListsPromises);
    const aliasListsObject = zipObject(lambdaNames, aliasLists);

    lambdaNames.forEach((lambdaName) => {
      console.log(`Evaluating: ${lambdaName} for old versions/aliases to retain. `);
      const aliases = aliasListsObject[lambdaName];
      const cumulusAliases = aliases.filter(
        (alias) => alias.Description.includes(cumulusAliasDescription)
      );

      if (cumulusAliases.length === 0) return;

      cumulusAliases.sort((a, b) => b.FunctionVersion - a.FunctionVersion);
      const oldAliases = cumulusAliases.filter(
        (alias) => this.parseAliasName(alias.Name).hash !== configLambdas[lambdaName].hash
      );
      const oldAliasMetadataObjects = oldAliases.map((alias) => (
        {
          name: alias.Name,
          humanReadableIdentifier: this.getHumanReadableIdentifier(alias.Description)
        }
      )).slice(0, numberOfRetainedLambdas);

      if (oldAliasMetadataObjects.length > 0) {
        console.log(
          'Adding the following "old" versions to LambdaVersions:',
          `${JSON.stringify(oldAliasMetadataObjects.map((obj) => obj.name))}`
        );
      }
      aliasMetadataObjects = aliasMetadataObjects.concat(oldAliasMetadataObjects);
    });
    return aliasMetadataObjects;
  }


  /**
   * Parses a passed in alias description field for a version string,
   * (e.g. `Cumulus Autogenerated Alias |version`)
   *
   * @param {string} description lambda alias description
   * @returns {string} Returns the human readable version or '' if no match is found
   */
  getHumanReadableIdentifier(description) {
    const descriptionMatch = description.match(/.*\|(.*)$/);
    if (!descriptionMatch) return '';
    return descriptionMatch[1] || '';
  }

  /**
   * Parses  Alias name properties into a results object
   *
   * @param {string} name - Cumulus created CF Lambda::Alias name parameter
   *                        in format Name-Hash,
   * @returns {Object} returns hash with name/value keys mapped to appropriate
   *                   matches and sets hash to null if no hash match in 'name'
   */
  parseAliasName(name) {
    const regExp = /^([^-]*)-([^-]*)$/;
    const regExpResults = regExp.exec(name);
    let hashValue = null;
    if (regExpResults[2]) hashValue = regExpResults[2];
    return { name: regExpResults[1], hash: hashValue };
  }

  /**
   * Uses getRetainedLambdaAliasMetadata to generate a list of lambda
   * aliases to save, then parses each name/hash pair to generate  CF template
   * configuration name: [hashes] and injects that into the oldLambdas config
   * key
   *
   * @returns {Promise.void} Returns nothing.
   */
  async injectOldWorkflowLambdaAliases() {
    const oldLambdaMetadataObjects = await this.getRetainedLambdaAliasMetadata();
    const oldLambdas = {};

    oldLambdaMetadataObjects.forEach((obj) => {
      const matchObject = this.parseAliasName(obj.name);
      if (matchObject.hash) {
        if (!oldLambdas[matchObject.name]) oldLambdas[matchObject.name] = { lambdaRefs: [] };
        oldLambdas[matchObject.name].lambdaRefs.push(
          {
            hash: matchObject.hash,
            humanReadableIdentifier: obj.humanReadableIdentifier
          }
        );
      }
    });
    this.config.oldLambdas = oldLambdas;
  }


  /**
   * Updates all this.config.stepFunctions state objects of type Task with
   * a LambdaFunction.ARN resource to refer to the a generated LambdaAlias
   * reference elsewhere in the template.
   *
   * Functions without a unique identifier (hash), and therefore no alias
   * will continue to utilize the original reference.
   *
   * @returns {void} Returns nothing.
   */
  injectWorkflowLambdaAliases() {
    console.log('Updating workflow Lambda ARN references to Lambda Alias references');
    Object.keys(this.config.stepFunctions).forEach((stepFunction) => {
      const stepFunctionStateKeys = Object.keys(this.config.stepFunctions[stepFunction].States);
      stepFunctionStateKeys.forEach((stepFunctionState) => {
        const stateObject = this.config.stepFunctions[stepFunction].States[stepFunctionState];

        if ((stateObject.Type === 'Task')
            && (stateObject.Resource.endsWith('LambdaFunction.Arn}'))) {
          const lambdaAlias = this.lookupLambdaReference(stateObject.Resource);
          console.log(`Updating workflow ${stateObject.Resource} reference to ${lambdaAlias}`);
          stateObject.Resource = lambdaAlias;
        }
      });
    });
  }


  /**
   * Programatically evaluates a lambda ARN reference and returns the expected template reference.
   * This will either be the unqualified Lambda reference if unique identifier exists, or a
   * reference to the expected LambdaAliasOutput key from the LambdaVersions subtemplate.
   *
   * @param {string} stateObjectResource - CF template resource reference for a state function
   * @returns {string} The correct reference to the lambda function, either a hashed alias
   * reference or the passed in resource if hasing/versioning isn't possible for this resource
   * @throws {Error} Throws an error if the passed in stateObjectResource isn't a LambdaFunctionArn
   * reference
   */
  lookupLambdaReference(stateObjectResource) {
    let lambdaKey;
    const regExp = /^\$\{(.*)LambdaFunction.Arn/;
    const matchArray = regExp.exec(stateObjectResource);

    if (matchArray) {
      lambdaKey = matchArray[1];
    }
    else {
      console.log(`Invalid workflow configuration, ${stateObjectResource} `
                  + 'is not a valid Lambda ARN');
      throw new Error(`Invalid stateObjectResource: ${stateObjectResource}`);
    }
    const lambdaHash = this.config.lambdas[lambdaKey].hash;
    // If a lambda resource doesn't have a hash, refer directly to the function ARN
    if (!lambdaHash) {
      console.log(`No unique identifier for ${lambdaKey}, referencing ${stateObjectResource}`);
      return (stateObjectResource);
    }

    return `\$\{${lambdaKey}LambdaAliasOutput\}`;
  }


  /**
   * Override opsStack method.
   *
   * @returns {Promise} aws response
   */
  opsStack() {
    // check if public and private key are generated
    // if not generate and upload them
    const apis = {};

    // remove config variable from all workflow steps
    // and keep them in a separate variable.
    // this is needed to prevent StepFunction deployment from crashing
    this.config = extractCumulusConfigFromSF(this.config);

    return crypto(this.stack, this.bucket, this.s3)
      .then(() => super.opsStack())
      .then(() => this.describeCF())
      .then((r) => {
        const outputs = r.Stacks[0].Outputs;

        const urls = {
          Api: 'token',
          Distribution: 'redirect'
        };
        console.log('\nHere are the important URLs for this deployment:\n');
        outputs.forEach((o) => {
          if (Object.keys(urls).includes(o.OutputKey)) {
            console.log(`${o.OutputKey}: `, o.OutputValue);
            console.log('Add this url to URS: ', `${o.OutputValue}${urls[o.OutputKey]}`, '\n');

            if (o.OutputKey === 'Distribution') {
              this.config.distribution_endpoint = o.OutputValue;
            }
          }

          switch (o.OutputKey) {
          case 'ApiId':
            apis.api = o.OutputValue;
            break;
          case 'DistributionId':
            apis.distribution = o.OutputValue;
            break;
          case 'ApiStage':
            apis.stageName = o.OutputValue;
            break;
          default:
            //nothing
          }
        });

        return generateTemplates(this.config, outputs, this.uploadToS3.bind(this));
      })
      .then(() => this.restartECSTasks(this.config))
      .then(() => this.redeployApiGateWay('api', apis.api, apis.stageName))
      .then(() => this.redeployApiGateWay('distribution', apis.distribution, apis.stageName))
      .catch((e) => {
        console.log(e);
        throw e;
      });
  }
}

module.exports = UpdatedKes;
