import {
  PRED_STATUS_INIT, PRED_STATUS_IN_MEMPOOL, PRED_STATUS_PUT_OK,
  PRED_STATUS_PUT_ERROR, PRED_STATUS_CONFIRMED_OK, PRED_STATUS_CONFIRMED_ERROR,
  PRED_STATUS_VERIFIABLE, PRED_STATUS_VERIFYING, PRED_STATUS_VERIFIED_OK,
  PRED_STATUS_VERIFIED_ERROR, PDG, SCS,
} from './const';

export const runAsyncWrapper = (callback) => {
  return function (req, res, next) {
    callback(req, res, next).catch(next);
  }
};

export const sleep = ms => {
  return new Promise(resolve => setTimeout(resolve, ms));
};

export const isObject = (val) => {
  return typeof val === 'object' && val !== null;
};

export const isString = (val) => {
  return typeof val === 'string' || val instanceof String;
};

export const isNumber = (val) => {
  return typeof val === 'number' && isFinite(val);
};

export const sample = (arr) => {
  return arr[Math.floor(Math.random() * arr.length)];
};

export const randomString = (length) => {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  const charactersLength = characters.length;

  let result = '';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
};

export const getPredStatus = (pred, burnHeight = null) => {
  if ('pStatus' in pred && ![PDG, SCS].includes(pred.pStatus)) {
    return PRED_STATUS_PUT_ERROR;
  }
  if ('cStatus' in pred && ![PDG, SCS].includes(pred.cStatus)) {
    return PRED_STATUS_CONFIRMED_ERROR;
  }
  if ('vStatus' in pred && ![PDG, SCS].includes(pred.vStatus)) {
    return PRED_STATUS_VERIFIED_ERROR;
  }

  if (pred.vStatus === SCS) return PRED_STATUS_VERIFIED_OK;
  if ('vTxId' in pred) return PRED_STATUS_VERIFYING;
  if (pred.cStatus === SCS) {
    if (
      isNumber(pred.targetBurnHeight) &&
      isNumber(burnHeight) &&
      pred.targetBurnHeight < burnHeight
    ) {
      return PRED_STATUS_VERIFIABLE;
    }
    return PRED_STATUS_CONFIRMED_OK;
  }
  if (pred.pStatus === SCS) return PRED_STATUS_PUT_OK;
  if ('cTxId' in pred) return PRED_STATUS_IN_MEMPOOL;
  return PRED_STATUS_INIT;
};

export const isNotNullIn = (entity, key) => {
  return key in entity && entity[key] !== null;
};
