'use strict';

const _ = require('lodash');
const { Kinesis } = require('aws-sdk');
const {
  aws: {
    sfn,
    receiveSQSMessages
  },
  waitForConditionalValue
} = require('@cumulus/common');

const {
  LambdaStep,
  getWorkflowArn,
  timeout
} = require('@cumulus/integration-tests');

const { loadConfig } = require('../helpers/testUtils');

const testConfig = loadConfig();

const lambdaStep = new LambdaStep();

const kinesis = new Kinesis({ apiVersion: '2013-12-02', region: testConfig.awsRegion });

const maxExecutionResults = 20;
const waitPeriodMs = 1000;

/**
 * Helper to simplify common setup code.  wraps function in try catch block
 * that will exit tests if the initial setup conditions fail.
 *
 * @param {Function} fn - function to execute
 * @param {iterable} args - arguments to pass to the function.
 * @returns {null} - no return
 */
function tryCatchExit(fn, ...args) {
  try {
    return fn.apply(this, args);
  }
  catch (error) {
    console.log(error);
    console.log('Tests conditions can\'t get met...exiting.');
    process.exit(1);
  }
  return null;
}


/**
 * returns the most recently executed KinesisTriggerTest workflows.
 *
 * @returns {Array<Object>} array of state function executions.
 */
async function getExecutions() {
  const kinesisTriggerTestStpFnArn = await getWorkflowArn(testConfig.stackName, testConfig.bucket, 'KinesisTriggerTest');
  const data = await sfn().listExecutions({
    stateMachineArn: kinesisTriggerTestStpFnArn,
    maxResults: maxExecutionResults
  }).promise();
  return (_.orderBy(data.executions, 'startDate', 'desc'));
}


/**
 * Wait for a number of periods for a kinesis stream to become active.
 *
 * @param {string} streamName - name of kinesis stream to wait for
 * @param {integer} maxNumberElapsedPeriods - number of periods to wait for stream
 *                  default value 30; duration of period is 1000ms
 * @returns {Promise<string>} current stream status: 'ACTIVE'
 */
function waitForActiveStream(streamName, maxNumberElapsedPeriods = 60) {
  const getStreamStatus = () =>
    kinesis.describeStream({ StreamName: streamName }).promise()
      .then((stream) => stream.StreamDescription.StreamStatus);

  return waitForConditionalValue(
    getStreamStatus,
    (status) => status === 'ACTIVE',
    {
      interval: waitPeriodMs,
      timeout: maxNumberElapsedPeriods * waitPeriodMs
    }
  );
}

/**
 * Helper function to delete a stream by name
 *
 * @param {string} streamName - name of kinesis stream to delete
 * @returns {Promise<Object>} - a kinesis delete stream proxy object.
 */
async function deleteTestStream(streamName) {
  return kinesis.deleteStream({ StreamName: streamName }).promise();
}

/**
 *  returns a active kinesis stream, creating it if necessary.
 *
 * @param {string} streamName - name of stream to return
 * @returns {Object} empty object if stream is created and ready.
 * @throws {Error} Kinesis error if stream cannot be created.
 */
async function createOrUseTestStream(streamName) {
  let stream;

  try {
    stream = await kinesis.describeStream({ StreamName: streamName }).promise();
  }
  catch (err) {
    if (err.code === 'ResourceNotFoundException') {
      console.log('Creating a new stream:', streamName);
      stream = await kinesis.createStream({ StreamName: streamName, ShardCount: 1 }).promise();
    }
    else {
      throw err;
    }
  }
  return stream;
}

/**
 * Gets the shard iterator for stream <streamName> using LATEST: Records written to
 * the stream after this shard iterator is retrieved will be returned by calls
 * to GetRecords. NOTE: Shard iterators expire after 5 minutes if not used in a
 * GetRecords call.
 *
 * @param  {string} streamName - Name of the stream of interest
 * @returns {string}            - Shard iterator
 */
async function getShardIterator(streamName) {
  const describeStreamParams = {
    StreamName: streamName
  };

  const streamDetails = await kinesis.describeStream(describeStreamParams).promise();
  const shardId = streamDetails.StreamDescription.Shards[0].ShardId;

  const shardIteratorParams = {
    ShardId: shardId, /* required */
    ShardIteratorType: 'LATEST',
    StreamName: streamName
  };

  const shardIterator = await kinesis.getShardIterator(shardIteratorParams).promise();
  return shardIterator.ShardIterator;
}

/**
 * Gets records from a kinesis stream using a shard iterator.
 *
 * @param  {string} shardIterator - Kinesis stream shard iterator.
 *                                  Shard iterators must be generated using getShardIterator.
 * @returns {Promise}              - kinesis GetRecords promise
 */
async function getRecords(shardIterator) {
  return kinesis.getRecords({ ShardIterator: shardIterator }).promise();
}

/**
 * add a record to the kinesis stream.
 *
 * @param {string} streamName - kinesis stream name
 * @param {Object} record - CNM object to drop on stream
 * @returns {Promise<Object>} - Kinesis putRecord response proxy object.
 */
async function putRecordOnStream(streamName, record) {
  return kinesis.putRecord({
    Data: JSON.stringify(record),
    PartitionKey: '1',
    StreamName: streamName
  }).promise();
}


/**
 * Wait for test stepfunction execution to exist.
 *
 * @param {string} recordIdentifier - random string identifying correct execution for test
 * @param {integer} maxWaitTime - maximum time to wait for the correct execution in milliseconds
 * @param {string} firstStep - The name of the first step of the workflow, used to query if the workflow has started.
 * @returns {Object} - {executionArn: <arn>, status: <status>}
 * @throws {Error} - any AWS error, re-thrown from AWS execution or 'Workflow Never Started'.
 */
async function waitForTestSf(recordIdentifier, maxWaitTime, firstStep = 'SfSnsReport') {
  const getExecutionOrNull = async () => {
    const executions = await getExecutions();
    // Search all recent executions for target recordIdentifier
    for (const execution of executions) {
      const taskInput = await lambdaStep.getStepInput(execution.executionArn, firstStep);
      if (taskInput !== null && taskInput.payload.identifier === recordIdentifier) {
        return execution;
      }
    }

    return null;
  };

  return waitForConditionalValue(
    getExecutionOrNull,
    (execution) => execution !== null,
    {
      interval: waitPeriodMs,
      timeout: maxWaitTime
    }
  );
}

/**
 * Return the original kinesis event embedded in an SQS message.
 *
 * @param {Object} message - SQS message
 * @returns {Object} kinesis object stored in SQS message.
 */
function kinesisEventFromSqsMessage(message) {
  let kinesisEvent;
  try {
    const originalKinesisMessage = JSON.parse(message.Body.Records[0].Sns.Message);
    const dataString = Buffer.from(originalKinesisMessage.kinesis.data, 'base64').toString();
    kinesisEvent = JSON.parse(dataString);
  }
  catch (error) {
    console.log('Error parsing KinesisEventFromSqsMessage(message)', JSON.stringify(message));
    console.log(error);
    kinesisEvent = { identifier: 'Fake Wrong Message' };
  }
  return kinesisEvent;
}

/**
 * Check if the returned SQS message holds the targeted kinesis record.
 *
 * @param {Object} message - SQS message.
 * @param {string} recordIdentifier - target kinesis record identifier.
 * @returns {Bool} - true, if this message contained the targeted record identifier.
 */
function isTargetMessage(message, recordIdentifier) {
  const kinesisEvent = kinesisEventFromSqsMessage(message);
  return kinesisEvent.identifier === recordIdentifier;
}

/**
 * Wait until a kinesisRecord appears in an SQS message who's identifier matches the input recordIdentifier.  Wait up to 10 minutes.
 *
 * @param {string} recordIdentifier - random string to match found messages against.
 * @param {string} queueUrl - kinesisFailure SQS url
 * @param {number} maxNumberElapsedPeriods - number of timeout intervals (5 seconds) to wait.
 * @returns {Object} - matched Message from SQS.
 */
async function waitForQueuedRecord(recordIdentifier, queueUrl, maxNumberElapsedPeriods = 120) {
  const getRecordOrNull = async () => {
    const messages = await receiveSQSMessages(queueUrl);
    if (messages.length > 0) {
      const targetMessage = messages.find((message) => isTargetMessage(message, recordIdentifier));
      if (targetMessage) return targetMessage;
    }
    return null;
  };

  return waitForConditionalValue(
    getRecordOrNull,
    (record) => record !== null,
    {
      interval: 5000,
      timeout: 5000 * maxNumberElapsedPeriods
    }
  )
    .catch((err) => {
      if (err.name === 'TimeoutError') {
        return { waitForQueuedRecord: 'never found record on queue' };
      }
      throw err;
    });
}

module.exports = {
  createOrUseTestStream,
  deleteTestStream,
  getShardIterator,
  getRecords,
  kinesisEventFromSqsMessage,
  putRecordOnStream,
  tryCatchExit,
  waitForActiveStream,
  waitForQueuedRecord,
  waitForTestSf
};
