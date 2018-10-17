'use strict';

const test = require('ava');

const aws = require('../../aws');
const { randomString } = require('../../test-utils');
const { S3Gateway } = require('../../S3Gateway');

const s3Service = aws.s3();

test('S3Gateway.objectExists() returns true if the object exists', async (t) => {
  const bucket = randomString();
  const key = `${randomString()}/${randomString()}`;

  try {
    await s3Service.createBucket({ Bucket: bucket }).promise();

    await s3Service.putObject({
      Bucket: bucket,
      Key: key,
      Body: 'my-body'
    }).promise();

    const s3Gateway = new S3Gateway(s3Service);

    t.true(await s3Gateway.objectExists(bucket, key));
  }
  finally {
    await aws.recursivelyDeleteS3Bucket(bucket);
  }
});

test('S3Gateway.objectExists() returns false if the object does not exist', async (t) => {
  const bucket = randomString();
  const key = `${randomString()}/${randomString()}`;

  try {
    await s3Service.createBucket({ Bucket: bucket }).promise();

    const s3Gateway = new S3Gateway(s3Service);

    t.false(await s3Gateway.objectExists(bucket, key));
  }
  finally {
    await aws.recursivelyDeleteS3Bucket(bucket);
  }
});
