'use strict';

const fs = require('fs-extra');
const path = require('path');
const test = require('ava');
const errors = require('@cumulus/common/errors');
const payload = require('@cumulus/test-data/payloads/new-message-schema/ingest.json');
const payloadChecksumFile = require('@cumulus/test-data/payloads/new-message-schema/ingest-checksumfile.json'); // eslint-disable-line max-len
const { constructCollectionId } = require('@cumulus/common');
const {
  checksumS3Objects,
  headObject,
  parseS3Uri,
  recursivelyDeleteS3Bucket,
  s3ObjectExists,
  s3,
  promiseS3Upload
} = require('@cumulus/common/aws');

const cloneDeep = require('lodash.clonedeep');
const {
  randomString,
  validateConfig,
  validateInput,
  validateOutput
} = require('@cumulus/common/test-utils');
const { syncGranule } = require('..');

// prepare the s3 event and data
async function prepareS3DownloadEvent(t) {
  const granuleFilePath = randomString();
  const granuleFileName = payload.input.granules[0].files[0].name;

  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };

  t.context.event.input.granules[0].files[0].path = granuleFilePath;
  t.context.event.config.fileStagingDir = randomString();

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  await s3().createBucket({ Bucket: t.context.event.config.provider.host }).promise();

  // Stage the file that's going to be downloaded
  const key = `${granuleFilePath}/${granuleFileName}`;
  await s3().putObject({
    Bucket: t.context.event.config.provider.host,
    Key: key,
    Body: fs.createReadStream(`../../packages/test-data/granules/${granuleFileName}`)
  }).promise();
}

// Setup buckets and the test event
test.beforeEach(async (t) => {
  t.context.internalBucketName = randomString();
  t.context.protectedBucketName = randomString();
  t.context.privateBucketName = randomString();

  await Promise.all([
    s3().createBucket({ Bucket: t.context.internalBucketName }).promise(),
    s3().createBucket({ Bucket: t.context.privateBucketName }).promise(),
    s3().createBucket({ Bucket: t.context.protectedBucketName }).promise()
  ]);

  const collection = payload.config.collection;
  // save collection in internal/stackName/collections/collectionId
  const key = `${process.env.stackName}/collections/${collection.dataType}___${parseInt(collection.version, 10)}.json`; // eslint-disable-line max-len
  await promiseS3Upload({
    Bucket: t.context.internalBucketName,
    Key: key,
    Body: JSON.stringify(collection),
    ACL: 'public-read'
  });

  t.context.event = cloneDeep(payload);

  t.context.event.config.downloadBucket = t.context.internalBucketName;
  t.context.event.config.buckets.internal = {
    name: t.context.internalBucketName,
    type: 'internal'
  };
  t.context.event.config.buckets.private = {
    name: t.context.privateBucketName,
    type: 'private'
  };
  t.context.event.config.buckets.protected = {
    name: t.context.protectedBucketName,
    type: 'protected'
  };
});

// Clean up
test.afterEach.always((t) => Promise.all([
  recursivelyDeleteS3Bucket(t.context.internalBucketName),
  recursivelyDeleteS3Bucket(t.context.privateBucketName),
  recursivelyDeleteS3Bucket(t.context.protectedBucketName)
]));

test.serial('error when provider info is missing', async (t) => {
  delete t.context.event.config.provider;

  try {
    await syncGranule(t.context.event);
    t.fail();
  }
  catch (error) {
    t.true(error instanceof errors.ProviderNotFound);
  }
});

test.serial('no error when collection info is not provided in the event', async (t) => {
  delete t.context.event.config.collection;
  // if not passed in the collection, this is required to be passed in context
  t.context.event.config.duplicateHandling = 'replace';
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: '127.0.0.1',
    username: 'testuser',
    password: 'testpass'
  };

  const output = await syncGranule(t.context.event);
  await validateOutput(t, output);
  t.is(output.granules.length, 1);
  t.is(output.granules[0].files.length, 1);
});

test.serial('download Granule from FTP endpoint', async (t) => {
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'ftp',
    host: '127.0.0.1',
    username: 'testuser',
    password: 'testpass'
  };

  t.context.event.config.collection.url_path = 'example/';

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  try {
    const output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 1);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/MOD09GQ.A2017224.h27v08.006.2017227165029.hdf` // eslint-disable-line max-len
    );
    t.truthy(output.granules[0].files[0].url_path);
  }
  catch (e) {
    if (e instanceof errors.RemoteResourceError) {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else throw e;
  }
});

test.serial('download Granule from HTTP endpoint', async (t) => {
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://127.0.0.1:3030'
  };
  t.context.event.input.granules[0].files[0].path = '/granules';

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  // await fs.mkdir(localGranulePath);
  try {
    const granuleFilename = t.context.event.input.granules[0].files[0].name;

    const output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 1);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/${granuleFilename}`
    );
  }
  catch (e) {
    if (e instanceof errors.RemoteResourceError) {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else throw e;
  }
});

test.serial('download Granule from SFTP endpoint', async (t) => {
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'sftp',
    host: '127.0.0.1',
    port: 2222,
    username: 'user',
    password: 'password'
  };

  t.context.event.input.granules[0].files[0].path = '/granules';

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  try {
    const granuleFilename = t.context.event.input.granules[0].files[0].name;

    const output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 1);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/${granuleFilename}`
    );
    t.is(
      true,
      await s3ObjectExists({
        Bucket: t.context.internalBucketName,
        Key: `${keypath}/${granuleFilename}`
      })
    );
  }
  catch (e) {
    if (e instanceof errors.RemoteResourceError) {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else throw e;
  }
});

test.serial('download granule from S3 provider', async (t) => {
  const granuleFilePath = randomString();
  const granuleFileName = payload.input.granules[0].files[0].name;

  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };

  t.context.event.input.granules[0].files[0].path = granuleFilePath;

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  await s3().createBucket({ Bucket: t.context.event.config.provider.host }).promise();

  try {
    // Stage the file that's going to be downloaded
    await s3().putObject({
      Bucket: t.context.event.config.provider.host,
      Key: `${granuleFilePath}/${granuleFileName}`,
      Body: fs.createReadStream(`../../packages/test-data/granules/${granuleFileName}`)
    }).promise();

    const output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 1);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/${granuleFileName}` // eslint-disable-line max-len
    );
    t.is(
      true,
      await s3ObjectExists({
        Bucket: t.context.internalBucketName,
        Key: `${keypath}/${granuleFileName}`
      })
    );
  }
  finally {
    // Clean up
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
});

test.serial('download granule with checksum in file from an HTTP endpoint', async (t) => {
  const event = cloneDeep(payloadChecksumFile);

  event.config.downloadBucket = t.context.internalBucketName;
  event.config.buckets.internal = {
    name: t.context.internalBucketName,
    type: 'internal'
  };
  event.config.buckets.private = {
    name: t.context.privateBucketName,
    type: 'private'
  };
  event.config.buckets.protected = {
    name: t.context.protectedBucketName,
    type: 'protected'
  };
  event.config.provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://127.0.0.1:3030'
  };

  event.input.granules[0].files[0].path = '/granules';
  event.input.granules[0].files[1].path = '/granules';

  await validateConfig(t, event.config);
  await validateInput(t, event.input);

  try {
    // Stage the files to be downloaded
    const granuleFilename = event.input.granules[0].files[0].name;

    const output = await syncGranule(event);

    await validateOutput(t, output);

    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 1);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/${granuleFilename}`
    );
    t.is(
      true,
      await s3ObjectExists({
        Bucket: t.context.internalBucketName,
        Key: `${keypath}/${granuleFilename}`
      })
    );
  }
  catch (e) {
    if (e instanceof errors.RemoteResourceError) {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else throw e;
  }
});

test.serial('download granule with bad checksum in file from HTTP endpoint throws', async (t) => {
  const granuleChecksumValue = 8675309;

  // Give it a bogus checksumValue to prompt a failure in validateChecksumFile
  t.context.event.input.granules[0].files[0].checksumValue = granuleChecksumValue;
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://127.0.0.1:3030'
  };

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  // Stage the files to be downloaded
  const granuleFilename = t.context.event.input.granules[0].files[0].name;
  const granuleChecksumType = t.context.event.input.granules[0].files[0].checksumType;
  const errorMessage = `Invalid checksum for ${granuleFilename} with type ${granuleChecksumType} and value ${granuleChecksumValue}`;// eslint-disable-line max-len

  await t.throws(syncGranule(t.context.event), errorMessage);
});

test.serial('validate file properties', async (t) => {
  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 'http',
    host: 'http://127.0.0.1:3030'
  };
  t.context.event.input.granules[0].files[0].path = '/granules';
  const [file] = t.context.event.input.granules[0].files;

  t.context.event.input.granules[0].files[1] = Object.assign({}, file, {
    name: 'MOD09GQ.A2017224.h27v08.006.2017227165029_1.jpg'
  });

  t.context.event.config.collection.files[0].url_path = 'file-example/';
  t.context.event.config.collection.url_path = 'collection-example/';

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  try {
    const granuleFilename = t.context.event.input.granules[0].files[0].name;
    const output = await syncGranule(t.context.event);

    await validateOutput(t, output);
    t.is(output.granules.length, 1);
    t.is(output.granules[0].files.length, 2);
    const config = t.context.event.config;
    const keypath = `file-staging/${config.stack}/${config.collection.dataType}___${parseInt(config.collection.version, 10)}`;// eslint-disable-line max-len
    t.is(
      output.granules[0].files[0].filename,
      `s3://${t.context.internalBucketName}/${keypath}/${granuleFilename}`
    );
    t.is(output.granules[0].files[0].url_path, 'file-example/');
    t.is(output.granules[0].files[1].url_path, 'collection-example/');
  }
  catch (e) {
    if (e instanceof errors.RemoteResourceError) {
      t.pass('ignoring this test. Test server seems to be down');
    }
    else throw e;
  }
});

test.serial('attempt to download file from non-existent path - throw error', async (t) => {
  const granuleFilePath = randomString();
  //const granuleFileName = payload.input.granules[0].files[0].name;

  t.context.event.config.provider = {
    id: 'MODAPS',
    protocol: 's3',
    host: randomString()
  };

  t.context.event.input.granules[0].files[0].path = granuleFilePath;

  await validateConfig(t, t.context.event.config);
  await validateInput(t, t.context.event.input);

  // Create the s3 bucket. If the bucket doesn't exist, we just get a
  // 'bucket doesn't exist' error
  await s3().createBucket({ Bucket: t.context.event.config.provider.host }).promise();

  try {
    await t.throws(syncGranule(t.context.event), null, 'Source file not found');

    // validateOutput will throw because it doesn't believe in error messages
    t.throws(() => {
      validateOutput(t, output);
    }, 'output is not defined');
  }
  finally {
    // Clean up
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
});

async function duplicateHandlingErrorTest(t) {
  await prepareS3DownloadEvent(t);
  const granuleFileName = payload.input.granules[0].files[0].name;

  try {
    const output = await syncGranule(t.context.event);
    await validateOutput(t, output);

    await syncGranule(t.context.event);
    t.fail();
  }
  catch (err) {
    const collection = payload.config.collection;
    const collectionId = constructCollectionId(collection.name, collection.version);
    const granuleFileKey = path.join(
      t.context.event.config.fileStagingDir,
      t.context.event.config.stack,
      collectionId,
      granuleFileName
    );
    t.true(err instanceof errors.DuplicateFile);
    t.is(
      err.message,
      `${granuleFileKey} already exists in ${t.context.event.config.downloadBucket} bucket`
    );
  }
  finally {
    // Clean up
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
}

test.serial('when duplicateHandling is not specified, throw an error on duplicate', async (t) => {
  await duplicateHandlingErrorTest(t);
});

test.serial('when duplicateHandling is "error", throw an error on duplicate', async (t) => {
  t.context.event.config.duplicateHandling = 'error';
  await duplicateHandlingErrorTest(t);
});

// TODO Fix this test as part of https://bugs.earthdata.nasa.gov/browse/CUMULUS-272
// test.cb('replace duplicate Granule', (t) => {
//   const provider = {
//     id: 'MODAPS',
//     protocol: 'http',
//     host: 'http://127.0.0.1:3030'
//   };
//   sinon.stub(S3, 'fileExists').callsFake(() => true);
//   const uploaded = sinon.stub(S3, 'upload').callsFake(() => '/test/test.hd');

//   const newPayload = cloneDeep(payload);
//   newPayload.provider = provider;
//   handler(newPayload, {}, (e, r) => {
//     S3.fileExists.restore();
//     S3.upload.restore();
//     if (e instanceof errors.RemoteResourceError) {
//       log.info('ignoring this test. Test server seems to be down');
//       return t.end();
//     }
//     t.true(uploaded.called);
//     return t.end(e);
//   });
// });

// TODO Fix this test as part of https://bugs.earthdata.nasa.gov/browse/CUMULUS-272
// test.cb('skip duplicate Granule', (t) => {
//   sinon.stub(S3, 'fileExists').callsFake(() => true);
//   const uploaded = sinon.stub(S3, 'upload').callsFake(() => '/test/test.hd');

//   const newPayload = cloneDeep(payload);
//   newPayload.config.collection.duplicateHandling = 'skip';
//   handler(newPayload, {}, (e, r) => {
//     S3.fileExists.restore();
//     S3.upload.restore();
//     if (e instanceof errors.RemoteResourceError) {
//       log.info('ignoring this test. Test server seems to be down');
//       return t.end();
//     }
//     t.false(uploaded.called);
//     return t.end(e);
//   });
// });

test.serial('when duplicateHandling is "version", keep both data if different', async (t) => {
  await prepareS3DownloadEvent(t);
  // duplicateHandling is taken from task config or collection config
  t.context.event.config.duplicateHandling = 'version';

  const granuleFileName = payload.input.granules[0].files[0].name;
  const granuleFilePath = t.context.event.input.granules[0].files[0].path;

  const key = `${granuleFilePath}/${granuleFileName}`;

  try {
    // staging the granule
    let output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    const existingFile = output.granules[0].files[0].filename;
    const existingFileInfo = await headObject(
      parseS3Uri(existingFile).Bucket, parseS3Uri(existingFile).Key
    );

    // stage the file with different content
    await s3().putObject({
      Bucket: t.context.event.config.provider.host,
      Key: key,
      Body: randomString()
    }).promise();

    t.context.event.input.granules[0].files[0].fileSize = granuleFileName.length;
    t.context.event.input.granules[0].files[0].checksumValue = await checksumS3Objects(
      t.context.event.input.granules[0].files[0].checksumType,
      t.context.event.config.provider.host,
      key
    );

    output = await syncGranule(t.context.event);
    await validateOutput(t, output);

    t.is(output.granules[0].files.length, 2);

    let filesNotRenamed = output.granules[0].files
      .filter((f) => path.basename(parseS3Uri(f.filename).Key) === granuleFileName);
    t.is(filesNotRenamed.length, 1);
    t.true(filesNotRenamed[0].duplicate_found);

    let filesRenamed = output.granules[0].files
      .filter((f) => path.basename(parseS3Uri(f.filename).Key).startsWith(`${granuleFileName}.v`));
    t.is(filesRenamed.length, 1);
    t.falsy(filesRenamed[0].duplicate_found);

    const renamedFileInfo = await headObject(
      parseS3Uri(filesRenamed[0].filename).Bucket, parseS3Uri(filesRenamed[0].filename).Key
    );
    t.deepEqual(existingFileInfo, renamedFileInfo);

    // stage the file again with different content
    await s3().putObject({
      Bucket: t.context.event.config.provider.host,
      Key: key,
      Body: randomString()
    }).promise();

    t.context.event.input.granules[0].files[0].fileSize = granuleFileName.length;
    t.context.event.input.granules[0].files[0].checksumValue = await checksumS3Objects(
      t.context.event.input.granules[0].files[0].checksumType,
      t.context.event.config.provider.host,
      key
    );

    output = await syncGranule(t.context.event);
    await validateOutput(t, output);

    t.is(output.granules[0].files.length, 3);

    filesNotRenamed = output.granules[0].files
      .filter((f) => path.basename(parseS3Uri(f.filename).Key) === granuleFileName);
    t.is(filesNotRenamed.length, 1);

    filesRenamed = output.granules[0].files
      .filter((f) => path.basename(parseS3Uri(f.filename).Key).startsWith(`${granuleFileName}.v`));
    t.is(filesRenamed.length, 2);
  }
  finally {
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
});

test.serial('when duplicateHandling is "skip", do not overwrite or create new', async (t) => {
  await prepareS3DownloadEvent(t);
  // duplicateHandling is taken from task config or collection config
  t.context.event.config.duplicateHandling = 'skip';

  const granuleFileName = payload.input.granules[0].files[0].name;
  const granuleFilePath = t.context.event.input.granules[0].files[0].path;

  const key = `${granuleFilePath}/${granuleFileName}`;

  try {
    // staging the granule
    let output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    const existingFile = output.granules[0].files[0].filename;
    const existingFileInfo = await headObject(
      parseS3Uri(existingFile).Bucket, parseS3Uri(existingFile).Key
    );

    // stage the file with different content
    await s3().putObject({
      Bucket: t.context.event.config.provider.host,
      Key: key,
      Body: randomString()
    }).promise();

    t.context.event.input.granules[0].files[0].fileSize = granuleFileName.length;
    t.context.event.input.granules[0].files[0].checksumValue = await checksumS3Objects(
      t.context.event.input.granules[0].files[0].checksumType,
      t.context.event.config.provider.host,
      key
    );

    output = await syncGranule(t.context.event);
    await validateOutput(t, output);

    t.is(output.granules[0].files.length, 1);
    t.true(output.granules[0].files[0].duplicate_found);

    const currentFile = output.granules[0].files[0].filename;
    const currentFileInfo = await headObject(
      parseS3Uri(currentFile).Bucket, parseS3Uri(currentFile).Key
    );
    t.deepEqual(existingFileInfo, currentFileInfo);
  }
  finally {
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
});

test.serial('when duplicateHandling is "replace", do overwrite files', async (t) => {
  await prepareS3DownloadEvent(t);
  // duplicateHandling is taken from task config or collection config
  t.context.event.config.duplicateHandling = 'replace';

  const granuleFileName = payload.input.granules[0].files[0].name;
  const granuleFilePath = t.context.event.input.granules[0].files[0].path;

  const key = `${granuleFilePath}/${granuleFileName}`;

  try {
    // staging the granule
    let output = await syncGranule(t.context.event);

    await validateOutput(t, output);

    const existingFile = output.granules[0].files[0].filename;
    const existingFileInfo = await headObject(
      parseS3Uri(existingFile).Bucket, parseS3Uri(existingFile).Key
    );

    // stage the file with different content
    await s3().putObject({
      Bucket: t.context.event.config.provider.host,
      Key: key,
      Body: randomString()
    }).promise();

    t.context.event.input.granules[0].files[0].fileSize = granuleFileName.length;
    t.context.event.input.granules[0].files[0].checksumValue = await checksumS3Objects(
      t.context.event.input.granules[0].files[0].checksumType,
      t.context.event.config.provider.host,
      key
    );

    output = await syncGranule(t.context.event);
    await validateOutput(t, output);

    t.is(output.granules[0].files.length, 1);
    t.true(output.granules[0].files[0].duplicate_found);

    const currentFile = output.granules[0].files[0].filename;
    const currentFileInfo = await headObject(
      parseS3Uri(currentFile).Bucket, parseS3Uri(currentFile).Key
    );
    t.is(currentFileInfo.ContentLength, randomString().length);
    t.true(currentFileInfo.LastModified >= existingFileInfo.LastModified);
  }
  finally {
    recursivelyDeleteS3Bucket(t.context.event.config.provider.host);
  }
});
