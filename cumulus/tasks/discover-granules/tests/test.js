'use strict';

import test from 'ava';
import sinon from 'sinon';
import log from '@cumulus/common/log';
import mur from '@cumulus/test-data/payloads/mur/discover.json';
import amsr2 from '@cumulus/test-data/payloads/amsr2/discover.json';
import queue from '@cumulus/ingest/queue';
import { handler } from '../index';

test.cb('test discovering mur granules', (t) => {
  const newMur = Object.assign({}, mur);

  // make sure queue is not used
  newMur.meta.useQueue = false;
  // update discovery rule
  const rule = '/allData/ghrsst/data/GDS2/L4/GLOB/JPL/MUR/v4.1/2017/(20[1-3])';
  newMur.collection.meta.provider_path = rule;

  handler(newMur, {}, (e, r) => {
    const granules = r.payload.granules;
    t.is(Object.keys(granules).length, 3);
    const g = Object.keys(granules)[0];
    t.is(granules[g].files.length, 2);
    t.end(e);
  });
});

test.cb('test discovering mur granules with queue', (t) => {
  const newMur = Object.assign({}, mur);
  sinon.stub(queue, 'queueGranule').callsFake(() => true);

  // update discovery rule
  const rule = '/allData/ghrsst/data/GDS2/L4/GLOB/JPL/MUR/v4.1/2017/(20[1-3])';
  newMur.meta = {};
  newMur.payload = {};
  newMur.collection.meta.provider_path = rule;

  handler(newMur, {}, (e, r) => {
    t.is(r.payload.granules_found, 3);
    queue.queueGranule.restore();
    t.end(e);
  });
});


test.cb('test discovering amsr2 granules using SFTP', (t) => {
  if (!process.env.JAXA_HOST || !process.env.JAXA_PORT) {
    log.info('Skipping SFTP test because credentials are not set');
    t.end();
  }
  else {
    const payload = Object.assign({}, amsr2);
    payload.provider.host = process.env.JAXA_HOST;
    payload.provider.port = process.env.JAXA_PORT;
    payload.provider.username = process.env.JAXA_USER;
    payload.provider.password = process.env.JAXA_PASS;
    payload.provider.encrypted = true;

    // update discovery rule
    payload.meta.useQueue = false;
    payload.payload = {};

    handler(payload, {}, (e, r) => {
      const granules = r.payload.granules;
      t.true(Object.keys(granules).length > 5);
      const g = Object.keys(granules)[0];
      t.is(granules[g].files.length, 1);
      t.end(e);
    });
  }
});