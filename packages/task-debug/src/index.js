'use strict';

// Let everyone know that this is a debug session
global.__isDebug = true;

const log = require('@cumulus/common/log');
const program = require('commander');
const local = require('@cumulus/common/local-helpers');
const workflow = require('./workflow');

const increaseVerbosity = (_v, total) => total + 1;

const doDebug = async () => {
  const configFile = program.configFile;
  const collectionId = program.collection;
  const workflowName = program.workflow;
  const bucket = program.bucket;

  log.info(`Config file: ${configFile}`);
  log.info(`Collection: ${collectionId}`);
  log.info(`Workflow: ${workflowName}`);
  log.info(`S3 Bucket: ${bucket}`);

  const workflows = local.parseWorkflows(collectionId);
  const wf = workflows[workflowName];
  const resources = {
    buckets: {
      private: {
        name: bucket,
        type: 'private'
      }
    }
  };

  const result = await workflow.runWorkflow(collectionId, wf, resources);

  log.info(`RESULT: ${JSON.stringify(result)}`);
};

program
  .version('0.0.1')
  .option('-v, --verbose', 'A value that can be increased', increaseVerbosity, 0)
  .option('-c, --collection <value>', 'The ID of the collection to process')
  .option('-w, --workflow <value>', 'The workflow to run')
  .option('-b, --bucket [value]', 'The private S3 bucket to use')
  .command('debugg <config-file>')
  .action(doDebug);

program.parse(process.argv);
