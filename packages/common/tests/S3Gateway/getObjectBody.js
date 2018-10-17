'use strict';

const test = require('ava');

const aws = require('../../aws');
const { randomString } = require('../../test-utils');
const {
  S3BucketNotFound,
  S3Gateway,
  S3ObjectNotFound
} = require('../../S3Gateway');

const s3Service = aws.s3();

test('S3Gateway.getObjectBody() returns the body of an S3 object as a string', async (t) => {
  const bucket = randomString();
  const key = `${randomString()}/${randomString()}`;

  await s3Service.createBucket({ Bucket: bucket }).promise();

  await s3Service.putObject({
    Bucket: bucket,
    Key: key,
    Body: 'my-body'
  }).promise();

  const s3Gateway = new S3Gateway(s3Service);

  const body = await s3Gateway.getObjectBody(bucket, key);

  t.is(body, 'my-body');
});

test('S3Gateway.getObjectBody() throws an S3BucketNotFound exception if the bucket does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();

  const s3Gateway = new S3Gateway(s3Service);

  try {
    await s3Gateway.getObjectBody(bucket, key);
    t.fail('Expected an S3BucketNotFound to be thrown');
  }
  catch (err) {
    t.true(err instanceof S3BucketNotFound, 'Expected an S3BucketNotFound to be thrown');
    t.true(err.message.includes(bucket), 'Expected the error message to include the bucket');
    t.is(err.bucket, bucket);
  }
});

test('S3Gateway.getObjectBody() throws an S3ObjectNotFound exception if the key does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();

  await s3Service.createBucket({ Bucket: bucket }).promise();

  const s3Gateway = new S3Gateway(s3Service);

  try {
    await s3Gateway.getObjectBody(bucket, key);
    t.fail('Expected an S3ObjectNotFound to be thrown');
  }
  catch (err) {
    t.true(err instanceof S3ObjectNotFound, 'Expected an S3ObjectNotFound to be thrown');
    t.true(err.message.includes(bucket), 'Expected the error message to include the bucket');
    t.true(err.message.includes(key), 'Expected the error message to include the key');
    t.is(err.bucket, bucket);
    t.is(err.key, key);
  }
});
