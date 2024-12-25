import { Datastore } from '@google-cloud/datastore';

import { TOTAL, PRED_STATUS_CONFIRMED_OK, ALL } from './const';
import { sleep, isObject, sample, getPredStatus, isNotNullIn } from './utils';

const datastore = new Datastore();

const _updateTotal = async (oldUser, newUser, oldPred, newPred) => {
  const doAdd = (
    getPredStatus(newPred) === PRED_STATUS_CONFIRMED_OK &&
    (oldPred === null || getPredStatus(oldPred) !== PRED_STATUS_CONFIRMED_OK)
  )
  if (!doAdd) return;

  const { appBtcAddr } = newUser;
  const { game, value: predValue } = newPred;

  const keyNames = [
    `${appBtcAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${appBtcAddr}-${game}-confirmed_ok-count-cont-day`,
    `${appBtcAddr}-${game}-confirmed_ok-max-cont-day`,
    `${appBtcAddr}-${predValue}-confirmed_ok-count`,
    `${appBtcAddr}-confirmed_ok-count-cont-day`,
    `${appBtcAddr}-confirmed_ok-max-cont-day`,
    `${game}-${predValue}-confirmed_ok-count`,
    `${game}-count-appBtcAddr`,
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'count-appBtcAddr',
  ];
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [entities] = await transaction.get(keys);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, isFirst, contDay;

    [keyName, key, entity, formula] = [keyNames[0], keys[0], entities[0], formulas[0]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, appBtcAddr, game, formula, 1, now, now);
      isFirst = true;
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = [keyNames[1], keys[1], entities[1], formulas[1]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= 24 * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, appBtcAddr, game, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    contDay = total.outcome;

    [keyName, key, entity, formula] = [keyNames[2], keys[2], entities[2], formulas[2]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < contDay) {
        [total.outcome, total.updateDate] = [contDay, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, appBtcAddr, game, formula, contDay, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[3], keys[3], entities[3], formulas[3]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, appBtcAddr, ALL, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = [keyNames[4], keys[4], entities[4], formulas[4]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= 24 * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, appBtcAddr, ALL, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    contDay = total.outcome;

    [keyName, key, entity, formula] = [keyNames[5], keys[5], entities[5], formulas[5]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < contDay) {
        [total.outcome, total.updateDate] = [contDay, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, appBtcAddr, ALL, formula, contDay, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = [keyNames[6], keys[6], entities[6], formulas[6]];
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, ALL, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    // We can know if this is a new user for this game by checking user+game exists.
    if (isFirst) {
      [keyName, key, entity, formula] = [keyNames[7], keys[7], entities[7], formulas[7]];
      if (isObject(entity)) {
        total = entityToTotal(entity);
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        total = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    // We can know if this is a new user by checking oldUser === null,
    //   but keep updating total users in Total might not good for performance.
    // Query count(*) from User is better.

    transaction.save(newEntities);
    await transaction.commit();
  } catch (e) {
    await transaction.rollback();
    throw e;
  }
};

const updateTotal = async (oldUser, newUser, oldPred, newPred) => {
  const nTries = 3;
  for (let currentTry = 1; currentTry <= nTries; currentTry++) {
    try {
      await _updateTotal(oldUser, newUser, oldPred, newPred);
      break;
    } catch (error) {
      if (currentTry < nTries) await sleep(sample([100, 200, 280, 350, 500]));
      else throw error;
    }
  }
};

const newTotal = (
  keyName, appBtcAddr, game, formula, outcome, createDate, updateDate, anchor = null
) => {
  const total = {
    keyName, appBtcAddr, game, formula, outcome, createDate, updateDate,
  };
  if (anchor !== null) total.anchor = anchor;
  return total;
};

const totalToEntityData = (total) => {
  const data = [
    { name: 'appBtcAddr', value: total.appBtcAddr },
    { name: 'game', value: total.game },
    { name: 'formula', value: total.formula },
    { name: 'outcome', value: total.outcome, excludeFromIndexes: true },
    { name: 'createDate', value: new Date(total.createDate) },
    { name: 'updateDate', value: new Date(total.updateDate) },
  ];
  if ('anchor' in total) {
    data.push({ name: 'anchor', value: total.anchor, excludeFromIndexes: true });
  }
  return data;
};

const entityToTotal = (entity) => {
  const total = {
    keyName: entity[datastore.KEY].name,
    appBtcAddr: entity.appBtcAddr,
    game: entity.game,
    formula: entity.formula,
    outcome: entity.outcome,
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
  };
  if (isNotNullIn(entity, 'anchor')) total.anchor = entity.anchor;

  return total;
};

const data = { updateTotal };

export default data;
