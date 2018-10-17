'use strict';

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

class S3Gateway {
  constructor(s3Service) {
    privates.set(this, { s3Service });
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
