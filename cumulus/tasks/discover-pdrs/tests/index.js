'use strict';

const test = require('ava');
const path = require('path');
const sinon = require('sinon');
const {
  ProviderNotFound,
  FTPError,
  RemoteResourceError
} = require('@cumulus/common/errors');
const { S3 } = require('@cumulus/ingest/aws');

const { discoverPdrs } = require('../index');
const input = require('./fixtures/input.json');

const aws = require('@cumulus/common/aws');
const testUtils = require('@cumulus/common/test-utils');

test('error when provider info is missing', (t) =>
  discoverPdrs({})
    .then(t.fail)
    .catch((e) => t.true(e instanceof ProviderNotFound)));

test('test pdr discovery with FTP assuming all PDRs are new', (t) => {
  const testInput = Object.assign({}, input);
  testInput.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: 'localhost',
    username: 'testuser',
    password: 'testpass'
  };
  testInput.config.collection.provider_path = '/pdrs';
  testInput.config.useQueue = false;

  const ps = {
    'MYD13A1_5_grans.PDR': false,
    'PDN.ID1611071307.PDR': false,
    'PDN.ID1611081200.PDR': false
  };
  sinon.stub(S3, 'fileExists').callsFake((b, k) => ps[path.basename(k)]);

  return discoverPdrs(testInput, {})
    .then((result) => {
      S3.fileExists.restore();
      t.is(result.pdrs.length, 4);
    })
    .catch((err) => {
      S3.fileExists.restore();
      if (err instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
      }
      else throw err;
    });
});

test('test pdr discovery with FTP invalid user/pass', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: 'localhost',
    username: 'testuser1',
    password: 'testpass'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.input = {};

  return discoverPdrs(newPayload, {})
    .then(t.fail)
    .catch((e) => {
      if (e instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
      }
      else {
        t.true(e instanceof FTPError);
        t.true(e.message.includes('Login incorrect'));
      }
    });
});

test('test pdr discovery with FTP connection refused', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: 'localhost',
    port: '30', // using port that doesn't exist to nonresponsiveness
    username: 'testuser1',
    password: 'testpass'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.input = {};

  return discoverPdrs(newPayload, {})
    .then(t.fail)
    .catch((e) => {
      t.true(e instanceof RemoteResourceError);
    });
});

test('test pdr discovery with FTP assuming some PDRs are new', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: 'localhost',
    username: 'testuser',
    password: 'testpass'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.config.useQueue = false;
  newPayload.config.collection.provider_path = '/pdrs';
  newPayload.input = {};

  const internalBucketName = testUtils.randomString();
  newPayload.config.buckets.internal = internalBucketName;
  return aws.s3().createBucket({ Bucket: internalBucketName }).promise()
    .then(() => {
      const Key = [
        newPayload.config.stack,
        newPayload.config.collection.provider_path.replace(/^\//, ''),
        'PDN.ID1611071307.PDR'
      ].join('/');

      return aws.s3().putObject({
        Bucket: internalBucketName,
        Key,
        Body: 'PDN.ID1611071307.PDR'
      }).promise();
    })
    .then(() => discoverPdrs(newPayload, {}))
    .then((output) => {
      t.is(output.pdrs.length, 3);
      return aws.recursivelyDeleteS3Bucket(internalBucketName);
    })
    .catch((e) => {
      if (e instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
        return aws.recursivelyDeleteS3Bucket(internalBucketName);
      }
      return aws.recursivelyDeleteS3Bucket(internalBucketName).then(t.fail);
    });
});

test('test pdr discovery with HTTP assuming some PDRs are new', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://localhost:8080'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.config.useQueue = false;
  newPayload.config.collection.provider_path = '/';
  newPayload.input = {};

  const internalBucketName = testUtils.randomString();
  newPayload.config.buckets.internal = internalBucketName;
  return aws.s3().createBucket({ Bucket: internalBucketName }).promise()
    .then(() => aws.s3().putObject({
      Bucket: internalBucketName,
      Key: 'lpdaac-cumulus-phaseIII/pdrs/pdrs/PDN.ID1611071307.PDR',
      Body: 'PDN.ID1611071307.PDR'
    }).promise())
    .then(() => discoverPdrs(newPayload, {}))
    .then((output) => {
      t.is(output.pdrs.length, 2);
      return aws.recursivelyDeleteS3Bucket(internalBucketName);
    })
    .catch((e) => {
      if (e instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
        return aws.recursivelyDeleteS3Bucket(internalBucketName);
      }
      return aws.recursivelyDeleteS3Bucket(internalBucketName).then(t.fail);
    });
});

test('test pdr discovery with SFTP assuming some PDRs are new', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 'sftp',
    host: 'localhost',
    port: 2222,
    username: 'user',
    password: 'password'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.config.useQueue = false;
  newPayload.config.collection.provider_path = 'test-data/pdrs';
  newPayload.input = {};

  const internalBucketName = testUtils.randomString();
  newPayload.config.buckets.internal = internalBucketName;
  return aws.s3().createBucket({ Bucket: internalBucketName }).promise()
    .then(() => aws.s3().putObject({
      Bucket: internalBucketName,
      Key: 'lpdaac-cumulus-phaseIII/pdrs/PDN.ID1611071307.PDR',
      Body: 'PDN.ID1611071307.PDR'
    }).promise())
    .then(() => discoverPdrs(newPayload, {}))
    .then((output) => {
      t.is(output.pdrs.length, 3);
      const names = output.pdrs.map(p => p.name);
      t.true(names.includes('MOD09GQ.PDR'));
      t.true(names.includes('MYD13A1_5_grans.PDR'));
      t.true(names.includes('PDN.ID1611081200.PDR'));
      return aws.recursivelyDeleteS3Bucket(internalBucketName);
    })
    .catch((e) => {
      if (e instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
        return aws.recursivelyDeleteS3Bucket(internalBucketName);
      }
      return aws.recursivelyDeleteS3Bucket(internalBucketName).then(t.fail);
    });
});

test('test pdr discovery with S3 assuming some PDRs are new', (t) => {
  const provider = {
    id: 'MODAPS',
    protocol: 's3'
  };

  const newPayload = Object.assign({}, input);
  newPayload.config.provider = provider;
  newPayload.config.useQueue = false;
  newPayload.config.collection.provider_path = 's3://MODAPS/test-data/pdrs';
  newPayload.input = {};

  // upload test data to s3
  const pdrs = [
    'MOD09GQ.PDR',
    'MYD13A1_5_grans.PDR',
    'this_is_not_PDR_file',
    'PDN.ID1611071307.PDR',
    'PDN.ID1611081200.PDR'
  ];
  pdrs.forEach((pdr) => {
    const params = aws.parseS3Uri(`${newPayload.config.collection.provider_path}/${pdr}`);
    aws.s3().createBucket({ Bucket: params.Bucket }).promise()
      .then(() => aws.s3().putObject({
        Bucket: params.Bucket,
        Key: params.Key,
        Body: 'test data'
      }).promise());
  });

  const internalBucketName = testUtils.randomString();
  newPayload.config.buckets.internal = internalBucketName;
  return aws.s3().createBucket({ Bucket: internalBucketName }).promise()
    .then(() => aws.s3().putObject({
      Bucket: internalBucketName,
      Key: 'lpdaac-cumulus-phaseIII/pdrs/PDN.ID1611071307.PDR',
      Body: 'PDN.ID1611071307.PDR'
    }).promise())
    .then(() => discoverPdrs(newPayload, {}))
    .then((output) => {
      t.is(output.pdrs.length, 3);
      const names = output.pdrs.map(p => p.name);
      t.true(names.includes('MOD09GQ.PDR'));
      t.true(names.includes('MYD13A1_5_grans.PDR'));
      t.true(names.includes('PDN.ID1611081200.PDR'));
      return aws.recursivelyDeleteS3Bucket(internalBucketName);
    })
    .catch((e) => {
      if (e instanceof RemoteResourceError) {
        t.pass('ignoring this test. Test server seems to be down');
        return aws.recursivelyDeleteS3Bucket(internalBucketName);
      }
      return aws.recursivelyDeleteS3Bucket(internalBucketName).then(t.fail);
    })
    .finally(() => {
      const params = aws.parseS3Uri(newPayload.config.collection.provider_path);
      aws.recursivelyDeleteS3Bucket(params.Bucket);
    });
});
