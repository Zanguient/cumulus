'use strict';

const get = require('lodash.get');
const aws = require('@cumulus/common/aws');
const Manager = require('./base');
const { constructCollectionId, parseException } = require('../lib/utils');
const executionSchema = require('./schemas').execution;

class Execution extends Manager {
  constructor() {
    super({
      tableName: process.env.ExecutionsTable,
      tableHash: { name: 'arn', type: 'S' },
      schema: executionSchema
    });
  }

  /**
   * Create a new execution record from incoming sns messages
   *
   * @param {Object} payload - sns message containing the output of a Cumulus Step Function
   * @returns {Promise<Object>} an execution record
   */
  createExecutionFromSns(payload) {
    const stateMachineArn = get(payload, 'cumulus_meta.state_machine');
    const executionName = get(payload, 'cumulus_meta.execution_name');
    const executionArn = aws.getExecutionArn(
      stateMachineArn,
      executionName
    );
    if (!executionArn) {
      const error = new Error('State Machine Arn is missing. Must be included in the cumulus_meta');
      return Promise.reject(error);
    }

    const execution = aws.getExecutionUrl(executionArn);
    const collectionId = constructCollectionId(
      get(payload, 'meta.collection.name'), get(payload, 'meta.collection.version')
    );

    const doc = {
      executionName,
      executionArn,
      parentArn: get(payload, 'cumulus_meta.parentExecutionArn'),
      execution,
      error: parseException(payload.exception),
      type: get(payload, 'meta.workflow_name'),
      collectionId: collectionId,
      status: get(payload, 'meta.status', 'unknown'),
      createdAt: get(payload, 'cumulus_meta.workflow_start_time'),
      timestamp: Date.now()
    };

    doc.duration = (doc.timestamp - doc.createdAt) / 1000;
    return this.create(doc);
  }
}

module.exports = Execution;
