'use strict';

const test = require('ava');
const {
  createQueue, sqs, s3, recursivelyDeleteS3Bucket
} = require('@cumulus/common/aws');
const { randomString } = require('@cumulus/common/test-utils');
const queue = require('../queue');

test.beforeEach(async (t) => {
  t.context.templateBucket = randomString();
  await s3().createBucket({ Bucket: t.context.templateBucket }).promise();

  t.context.queueUrl = await createQueue();

  t.context.stateMachineArn = randomString();

  t.context.messageTemplate = {
    cumulus_meta: {
      state_machine: t.context.stateMachineArn
    },
    meta: { queues: { startSF: t.context.queueUrl } }
  };

  const messageTemplateKey = `${randomString()}/template.json`;
  t.context.messageTemplateKey = messageTemplateKey;
  await s3().putObject({
    Bucket: t.context.templateBucket,
    Key: messageTemplateKey,
    Body: JSON.stringify(t.context.messageTemplate)
  }).promise();

  t.context.template = `s3://${t.context.templateBucket}/${messageTemplateKey}`;
});

test.afterEach(async (t) => {
  await Promise.all([
    recursivelyDeleteS3Bucket(t.context.templateBucket),
    sqs().deleteQueue({ QueueUrl: t.context.queueUrl }).promise()
  ]);
});

test.serial(
  'the queue receives a correctly formatted workflow message without a PDR', async (t) => {
    const granule = { granuleId: '1', files: [] };
    const { queueUrl } = t.context;
    const templateUri = `s3://${t.context.templateBucket}/${t.context.messageTemplateKey}`;
    const collection = { name: 'test-collection', version: '0.0.0' };
    const provider = { id: 'test-provider' };

    const output = await queue
      .enqueueGranuleIngestMessage(granule, queueUrl, templateUri, provider, collection);
    await sqs().receiveMessage({
      QueueUrl: t.context.queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1
    }).promise()
      .then((receiveMessageResponse) => {
        t.is(receiveMessageResponse.Messages.length, 1);

        const actualMessage = JSON.parse(receiveMessageResponse.Messages[0].Body);
        const expectedMessage = {
          cumulus_meta: {
            state_machine: t.context.stateMachineArn
          },
          meta: {
            queues: { startSF: t.context.queueUrl },
            provider: provider,
            collection: collection
          },
          payload: { granules: [granule] }
        };
        t.truthy(actualMessage.cumulus_meta.execution_name);
        t.true(output.endsWith(actualMessage.cumulus_meta.execution_name));
        expectedMessage.cumulus_meta.execution_name = actualMessage.cumulus_meta.execution_name;
        t.deepEqual(expectedMessage, actualMessage);
      });
  }
);

test.serial('the queue receives a correctly formatted workflow message with a PDR', async (t) => {
  const granule = { granuleId: '1', files: [] };
  const { queueUrl } = t.context;
  const templateUri = `s3://${t.context.templateBucket}/${t.context.messageTemplateKey}`;
  const collection = { name: 'test-collection', version: '0.0.0' };
  const provider = { id: 'test-provider' };
  const pdr = { name: randomString(), path: randomString() };
  const arn = randomString();

  const output = await queue
    .enqueueGranuleIngestMessage(granule, queueUrl, templateUri, provider, collection, pdr, arn);
  await sqs().receiveMessage({
    QueueUrl: t.context.queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 1
  }).promise()
    .then((receiveMessageResponse) => {
      t.is(receiveMessageResponse.Messages.length, 1);

      const actualMessage = JSON.parse(receiveMessageResponse.Messages[0].Body);
      const expectedMessage = {
        cumulus_meta: {
          state_machine: t.context.stateMachineArn,
          parentExecutionArn: arn
        },
        meta: {
          queues: { startSF: t.context.queueUrl },
          provider: provider,
          collection: collection,
          pdr: pdr
        },
        payload: { granules: [granule] }
      };
      t.truthy(actualMessage.cumulus_meta.execution_name);
      t.true(output.endsWith(actualMessage.cumulus_meta.execution_name));
      expectedMessage.cumulus_meta.execution_name = actualMessage.cumulus_meta.execution_name;
      t.deepEqual(expectedMessage, actualMessage);
    });
});
