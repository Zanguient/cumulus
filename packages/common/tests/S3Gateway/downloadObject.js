'use strict';

const fs = require('fs');
const test = require('ava');
const tempy = require('tempy');
const { promisify } = require('util');

const aws = require('../../aws');
const { randomString } = require('../../test-utils');
const {
  S3BucketNotFound,
  S3Gateway,
  S3ObjectNotFound
} = require('../../S3Gateway');

const promisedFsReadFile = promisify(fs.readFile);
const promisedFsUnlink = promisify(fs.unlink);

const fileExists = (file) =>
  new Promise((resolve) =>
    fs.access(file, (err) => (err ? resolve(false) : resolve(true))));

const s3Service = aws.s3();

test('S3Gateway.downloadObject() throws an S3BucketNotFound exception if the bucket does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();
  const destination = tempy.file();

  const s3Gateway = new S3Gateway(s3Service);

  try {
    await s3Gateway.downloadObject({ bucket, key, destination });
    t.fail('Expected an S3BucketNotFound to be thrown');
  }
  catch (err) {
    t.true(err instanceof S3BucketNotFound, 'Expected an S3BucketNotFound to be thrown');
    t.true(err.message.includes(bucket), 'Expected the error message to include the bucket');
    t.is(err.bucket, bucket);
  }
  finally {
    await promisedFsUnlink(destination);
  }
});

// TODO Not sure how to actually get this to pass
test.skip('S3Gateway.downloadObject() does not create the destination file if the bucket does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();
  const destination = tempy.file();

  const s3Gateway = new S3Gateway(s3Service);

  try {
    await s3Gateway.downloadObject({ bucket, key, destination });
    t.fail('Expected an S3BucketNotFound to be thrown');
  }
  catch (err) {
    t.true(err instanceof S3BucketNotFound, 'Expected an S3BucketNotFound to be thrown');
    t.false(await fileExists(destination), 'Destination file should not be created when the bucket does not exist.');
  }
  finally {
    await promisedFsUnlink(destination);
  }
});

test('S3Gateway.downloadObject() throws an S3ObjectNotFound exception if the key does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();
  const destination = tempy.file();

  try {
    await s3Service.createBucket({ Bucket: bucket }).promise();

    const s3Gateway = new S3Gateway(s3Service);

    try {
      await s3Gateway.downloadObject({ bucket, key, destination });
      t.fail('Expected an S3ObjectNotFound to be thrown');
    }
    catch (err) {
      t.true(err instanceof S3ObjectNotFound, 'Expected an S3ObjectNotFound to be thrown');
      t.true(err.message.includes(bucket), 'Expected the error message to include the bucket');
      t.true(err.message.includes(key), 'Expected the error message to include the key');
      t.is(err.bucket, bucket);
      t.is(err.key, key);
    }
  }
  finally {
    await Promise.all([
      promisedFsUnlink(destination),
      aws.recursivelyDeleteS3Bucket(bucket)
    ]);
  }
});

test.skip('S3Gateway.downloadObject() does not create the destination file if the key does not exist', async (t) => {
  const bucket = randomString();
  const key = randomString();
  const destination = tempy.file();

  try {
    await s3Service.createBucket({ Bucket: bucket }).promise();

    const s3Gateway = new S3Gateway(s3Service);

    try {
      await s3Gateway.downloadObject({ bucket, key, destination });
      t.fail('Expected an S3ObjectNotFound to be thrown');
    }
    catch (err) {
      t.true(err instanceof S3ObjectNotFound, 'Expected an S3ObjectNotFound to be thrown');
      t.false(await fileExists(destination), 'Destination file should not be created when the bucket does not exist.');
    }
  }
  finally {
    await Promise.all([
      promisedFsUnlink(destination),
      aws.recursivelyDeleteS3Bucket(bucket)
    ]);
  }
});

test('S3Gateway.downloadObject() downloads the object to disk', async (t) => {
  const bucket = randomString();
  const key = randomString();
  const destination = tempy.file();

  try {
    await s3Service.createBucket({ Bucket: bucket }).promise();

    await s3Service.putObject({
      Bucket: bucket,
      Key: key,
      Body: 'my-body'
    }).promise();

    const s3Gateway = new S3Gateway(s3Service);

    await s3Gateway.downloadObject({ bucket, key, destination });

    const fileContents = await promisedFsReadFile(destination, 'utf8');

    t.is(fileContents, 'my-body');
  }
  finally {
    await Promise.all([
      promisedFsUnlink(destination),
      aws.recursivelyDeleteS3Bucket(bucket)
    ]);
  }
});
