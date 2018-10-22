/* eslint no-template-curly-in-string: "off" */

'use strict';

const sinon = require('sinon');
const test = require('ava');

const configFixture = require('./fixtures/config.json');
const aliasFixture = require('./fixtures/aliases.json');
const UpdatedKes = require('../lib/kes');

test.beforeEach((t) => {
  t.context.kes = new UpdatedKes(configFixture);
});

function setupKesForLookupLambdaReference(kes, functionName, hashValue) {
  kes.config.lambdas = { [functionName]: { hash: hashValue } };
  return kes;
}

test('lookupLambdaReference returns the expected alias resource string', (t) => {
  const resourceString = '${FunctionNameLambdaFunction.Arn}';
  const kes = setupKesForLookupLambdaReference(t.context.kes, 'FunctionName', 'notarealhash');
  const result = kes.lookupLambdaReference(resourceString);
  t.is(result, '${FunctionNameLambdaAliasOutput}');
});

test('lookupLambdaReference returns the original ', (t) => {
  const resourceString = '${FunctionNameLambdaFunction.Arn}';
  const kes = setupKesForLookupLambdaReference(t.context.kes, 'FunctionName', null);
  const result = kes.lookupLambdaReference(resourceString);
  t.is(result, resourceString);
});

test('lookupLambdaReference throws error on invalid configuration', (t) => {
  const resourceString = '${SomeOtherConfigurationString.foobar}';
  const kes = setupKesForLookupLambdaReference(t.context.kes, 'FunctionName', null);
  const error = t.throws(() => {
    kes.lookupLambdaReference(resourceString);
  }, Error);
  t.is(error.message, `Invalid stateObjectResource: ${resourceString}`);
});

test('injectWorkflowLambdaAliases updates the correct resources', (t) => {
  const kes = t.context.kes;

  kes.config.lambdas = {
    TestLambda: { hash: 'notarealhash' },
    TestUnversionedLambda: { hash: null }
  };

  kes.config.stepFunctions = {
    TestStepFunction: {
      States: {
        1: { Type: 'Task', Resource: '${TestLambdaLambdaFunction.Arn}' },
        2: { Type: 'Task', Resource: '${TestUnversionedLambdaLambdaFunction.Arn}' },
        3: { Type: 'Task', Resource: '${SomethingElse.Arn}' }
      }
    },
    TestStepFunction2: {
      States: {
        1: { Type: 'Task', Resource: '${TestLambdaLambdaFunction.Arn}' }
      }
    }
  };

  const expected = {
    TestStepFunction: {
      States: {
        1: { Type: 'Task', Resource: '${TestLambdaLambdaAliasOutput}' },
        2: { Type: 'Task', Resource: '${TestUnversionedLambdaLambdaFunction.Arn}' },
        3: { Type: 'Task', Resource: '${SomethingElse.Arn}' }
      }
    },
    TestStepFunction2: {
      States: {
        1: { Type: 'Task', Resource: '${TestLambdaLambdaAliasOutput}' }
      }
    }
  };

  kes.injectWorkflowLambdaAliases();
  t.deepEqual(expected, kes.config.stepFunctions);
});


test('injectOldWorkflowLambdaAliases adds oldLambdas to configuration object', async (t) => {
  const kes = t.context.kes;
  const nameArray = [
    {
      name: 'LambdaName-12345',
      humanReadableIdentifier: 'id12345'
    },
    {
      name: 'LambdaName-abcde',
      humanReadableIdentifier: 'idabcde'
    },
    {
      name: 'SecondLambdaName-67890',
      humanReadableIdentifier: 'id67890'
    }
  ];

  kes.getRetainedLambdaAliasMetadata = () => (Promise.resolve(nameArray));

  const expected = {
    LambdaName: {
      lambdaRefs:
      [
        {
          hash: '12345',
          humanReadableIdentifier: 'id12345'
        },
        {
          hash: 'abcde',
          humanReadableIdentifier: 'idabcde'
        }
      ]
    },
    SecondLambdaName:
    {
      lambdaRefs:
      [
        {
          hash: '67890',
          humanReadableIdentifier: 'id67890'
        }
      ]
    }
  };

  await kes.injectOldWorkflowLambdaAliases();
  t.deepEqual(expected, kes.config.oldLambdas);
});


test('parseAliasName parses the alias name', (t) => {
  const expected = { name: 'functionName', hash: 'hashValue' };
  const actual = t.context.kes.parseAliasName('functionName-hashValue');
  t.deepEqual(expected, actual);
});

test('parseAliasName returns null hash if hash missing', (t) => {
  const expected = { name: 'functionName', hash: null };
  const actual = t.context.kes.parseAliasName('functionName-');
  t.deepEqual(expected, actual);
});


test.serial('getAllLambdaAliases returns an unpaginated list of aliases', async (t) => {
  const kes = t.context.kes;

  // Mock out AWS calls
  const versionUpAliases = aliasFixture.aliases[0];

  const listAliasesStub = sinon.stub().callsFake(() => Promise.reject());

  sinon.stub(kes.AWS, 'Lambda').callsFake(() => (
    {
      listAliases: (_config) => (
        { promise: listAliasesStub }
      )
    }
  ));

  for (let i = 0; i < versionUpAliases.length - 1; i += 1) {
    listAliasesStub.onCall(i).returns(Promise.resolve({
      NextMarker: 'mocked out',
      Aliases: [versionUpAliases[i]]
    }));
  }
  listAliasesStub.onCall(versionUpAliases.length - 1).returns(
    Promise.resolve({ Aliases: [versionUpAliases[versionUpAliases.length - 1]] })
  );

  const lambda = kes.AWS.Lambda();
  const config = { MaxItems: 1, FunctionName: 'Mocked' };

  const actual = await kes.getAllLambdaAliases(lambda, config);

  t.deepEqual(versionUpAliases, actual,
    `Expected:${JSON.stringify(versionUpAliases)}\n\n`
    + `Actual:${JSON.stringify(actual)}`);
});

test.serial('getRetainedLambdaAliasMetadata returns filtered aliasNames', async (t) => {
  const kes = t.context.kes;

  kes.config.workflowLambdas = aliasFixture.workflowLambdas;
  kes.config.maxNumberOfRetainedLambdas = 2;
  const getAllLambdaAliasesStub = sinon.stub(kes, 'getAllLambdaAliases');
  for (let i = 0; i < aliasFixture.aliases.length; i += 1) {
    getAllLambdaAliasesStub.onCall(i).returns(aliasFixture.aliases[i]);
  }

  const expected = [
    {
      name: 'VersionUpTest-PreviousVersionHash',
      humanReadableIdentifier: 'humanReadableVersion13'
    },
    {
      name: 'VersionUpTest-SecondPreviousVersionHash',
      humanReadableIdentifier: 'humanReadableVersion12'
    },
    {
      name: 'HelloWorld-d49d272b8b1e8eb98a61affc34b1732c1032b1ca',
      humanReadableIdentifier: 'humanReadableVersion13'
    }
  ];

  const actual = await kes.getRetainedLambdaAliasMetadata();
  t.deepEqual(expected, actual);
});

test.serial('getRetainedLambdaAliasNames returns filtered aliasNames '
            + 'on previous version redeployment', async (t) => {
  const kes = t.context.kes;

  kes.config.workflowLambdas = aliasFixture.workflowLambdas;
  kes.config.workflowLambdas.VersionUpTest.hash = 'PreviousVersionHash';
  const getAllLambdaAliasesStub = sinon.stub(kes, 'getAllLambdaAliases');
  for (let i = 0; i < aliasFixture.aliases.length; i += 1) {
    getAllLambdaAliasesStub.onCall(i).returns(aliasFixture.aliases[i]);
  }

  const expected = [
    {
      name: 'VersionUpTest-LatestVersionHash',
      humanReadableIdentifier: 'humanReadableVersion14'
    },
    {
      name: 'VersionUpTest-SecondPreviousVersionHash',
      humanReadableIdentifier: 'humanReadableVersion12'
    },
    {
      name: 'HelloWorld-d49d272b8b1e8eb98a61affc34b1732c1032b1ca',
      humanReadableIdentifier: 'humanReadableVersion13'
    }
  ];
  const actual = await kes.getRetainedLambdaAliasMetadata();
  t.deepEqual(expected, actual);
});

test.serial('getHumanReadableIdentifier returns a packed ID from the descriptiohn string', (t) => {
  const testString = 'Some Description Here|version';
  const expected = 'version';
  const actual = t.context.kes.getHumanReadableIdentifier(testString);
  t.is(expected, actual);
});

test.serial("getHumanReadableIdentifier returns '' for a version string with no packed ID", (t) => {
  const testString = 'Some Bogus Version String';
  const expected = '';
  const actual = t.context.kes.getHumanReadableIdentifier(testString);
  t.is(expected, actual);
});


test.serial('setParentOverrideConfigValues merges defined parent configuration', (t) => {
  const parentConfig = { overrideKey: true };
  const kes = t.context.kes;
  kes.config.overrideKey = false;
  kes.config.override_with_parent = ['overrideKey'];
  kes.config.parent = parentConfig;

  kes.setParentOverrideConfigValues();
  const expected = true;
  const actual = kes.config.overrideKey;

  t.is(expected, actual);
});

test.serial('setParentOverrideConfigValues ignores missing parent configuration', (t) => {
  const parentConfig = {};
  const kes = t.context.kes;
  kes.config.overrideKey = false;
  kes.config.override_with_parent = ['overrideKey'];
  kes.config.parent = parentConfig;

  kes.setParentOverrideConfigValues();
  const expected = false;
  const actual = kes.config.overrideKey;

  t.is(expected, actual);
});
