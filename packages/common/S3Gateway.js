'use strict';

const fs = require('fs');
const pump = require('pump');

class S3BucketNotFound extends Error {
  constructor(bucket) {
    super(`S3 Bucket not found: ${bucket}`);
    this.bucket = bucket;
    this.name = this.constructor.name;
  }
}

class S3ObjectNotFound extends Error {
  constructor(bucket, key) {
    super(`S3 Object not found: s3://${bucket}/${key}`);
    this.bucket = bucket;
    this.key = key;
    this.name = this.constructor.name;
  }
}

const privates = new WeakMap();

/**
 * The S3 interface we wish we had.
 */
class S3Gateway {
  constructor(s3Service) {
    privates.set(this, { s3Service });
  }

  /**
   * Save an S3 Object to disk
   *
   * @param {Object} params
   * @param {string} params.bucket - the source bucket
   * @param {string} params.key - the source key
   * @param {string} params.destination - the destination filename
   * @returns {Promise<string>} the destination filename
   */
  async downloadObject(params = {}) {
    const { bucket, key, destination } = params;

    const { s3Service } = privates.get(this);

    return new Promise((resolve, reject) => {
      const objectReadStream = s3Service.getObject({
        Bucket: bucket,
        Key: key
      }).createReadStream();

      const fileWriteStream = fs.createWriteStream(destination);

      pump(objectReadStream, fileWriteStream, (err) => {
        if (err) {
          if (err.name === 'NoSuchBucket') {
            reject(new S3BucketNotFound(bucket));
          }
          else if (err.name === 'NoSuchKey') {
            reject(new S3ObjectNotFound(bucket, key));
          }
          else {
            reject(err);
          }
        }
        else resolve(destination);
      });
    });
  }

  /**
   * Get the body of an S3 Object as a string
   *
   * @param {string} bucket - the Object's bucket
   * @param {string} key - the Object's key
   * @returns {Promise<string>} the body of the S3 object
   */
  async getObjectBody(bucket, key) {
    const { s3Service } = privates.get(this);

    try {
      const { Body } = await s3Service.getObject({
        Bucket: bucket,
        Key: key
      }).promise();

      return Body.toString();
    }
    catch (err) {
      if (err.name === 'NoSuchBucket') {
        throw new S3BucketNotFound(bucket);
      }

      if (err.name === 'NoSuchKey') {
        throw new S3ObjectNotFound(bucket, key);
      }

      throw err;
    }
  }

  /**
   * Test if an S3 Object exists
   *
   * @param {string} bucket - the Object's bucket
   * @param {string} key - the Object's key
   * @returns {Promise<boolean>}
   */
  async objectExists(bucket, key) {
    const { s3Service } = privates.get(this);

    try {
      await s3Service.headObject({
        Bucket: bucket,
        Key: key
      }).promise();

      return true;
    }
    catch (err) {
      if (err.code === 'NotFound') return false;

      throw err;
    }
  }
}

module.exports = {
  S3BucketNotFound,
  S3Gateway,
  S3ObjectNotFound
};
