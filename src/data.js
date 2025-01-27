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

  const { stxAddr, game, value: predValue } = newPred;

  const keyNames = [
    `${stxAddr}-${game}-${predValue}-confirmed_ok-count`,
    `${stxAddr}-${game}-confirmed_ok-count-cont-day`,
    `${stxAddr}-${game}-confirmed_ok-max-cont-day`,
    `${stxAddr}-${predValue}-confirmed_ok-count`,
    `${stxAddr}-confirmed_ok-count-cont-day`,
    `${stxAddr}-confirmed_ok-max-cont-day`,
    `${game}-${predValue}-confirmed_ok-count`,
    `${game}-count-stxAddr`,
  ];
  const formulas = [
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'confirmed_ok-count-cont-day',
    'confirmed_ok-max-cont-day',
    `${predValue}-confirmed_ok-count`,
    'count-stxAddr',
  ];
  const keys = keyNames.map(kn => datastore.key([TOTAL, kn]));

  const transaction = datastore.transaction();
  try {
    await transaction.run();

    const [_entities] = await transaction.get(keys);
    const entities = mapEntities(keyNames, _entities);

    const newEntities = [], now = Date.now();
    let keyName, key, entity, formula, total, isFirst, countCont;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 0);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, game, formula, 1, now, now);
      isFirst = true;
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 1);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, stxAddr, game, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 2);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, game, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 3);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 4);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (newPred.createDate - total.anchor <= (18 + 24) * 60 * 60 * 1000) {
        [total.outcome, total.anchor] = [total.outcome + 1, newPred.createDate];
      } else {
        [total.outcome, total.anchor] = [1, newPred.createDate];
      }

      total.updateDate = now;
    } else {
      total = newTotal(
        keyName, stxAddr, ALL, formula, 1, now, now, newPred.createDate
      );
    }
    newEntities.push({ key, data: totalToEntityData(total) });
    countCont = total.outcome;

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 5);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      if (total.outcome < countCont) {
        [total.outcome, total.updateDate] = [countCont, now];
        newEntities.push({ key, data: totalToEntityData(total) });
      }
    } else {
      total = newTotal(keyName, stxAddr, ALL, formula, countCont, now, now);
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 6);
    if (isObject(entity)) {
      total = entityToTotal(entity);
      [total.outcome, total.updateDate] = [total.outcome + 1, now];
    } else {
      total = newTotal(keyName, ALL, game, formula, 1, now, now);
    }
    newEntities.push({ key, data: totalToEntityData(total) });

    // We can know if this is a new user for this game by checking user+game exists.
    if (isFirst) {
      [keyName, key, entity, formula] = getAt(keyNames, keys, entities, formulas, 7);
      if (isObject(entity)) {
        total = entityToTotal(entity);
        [total.outcome, total.updateDate] = [total.outcome + 1, now];
      } else {
        total = newTotal(keyName, ALL, game, formula, 1, now, now);
      }
      newEntities.push({ key, data: totalToEntityData(total) });
    }

    // We can know if this is a new user by checking oldUser === null,
    //   on the first upload of pred (status in mempool)
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

const getAt = (keyNames, keys, entities, formulas, i) => {
  return [keyNames[i], keys[i], entities[i], formulas[i]];
};

const newTotal = (
  keyName, stxAddr, game, formula, outcome, createDate, updateDate, anchor = null
) => {
  const total = {
    keyName, stxAddr, game, formula, outcome, createDate, updateDate,
  };
  if (anchor !== null) total.anchor = anchor;
  return total;
};

const totalToEntityData = (total) => {
  const data = [
    { name: 'stxAddr', value: total.stxAddr },
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
    stxAddr: entity.stxAddr,
    game: entity.game,
    formula: entity.formula,
    outcome: entity.outcome,
    createDate: entity.createDate.getTime(),
    updateDate: entity.updateDate.getTime(),
  };
  if (isNotNullIn(entity, 'anchor')) total.anchor = entity.anchor;

  return total;
};

const mapEntities = (keyNames, _entities) => {
  const knToEtt = {};
  for (const ett of _entities) {
    if (!isObject(ett)) continue;
    knToEtt[ett[datastore.KEY].name] = ett;
  }

  const entities = [];
  for (const keyName of keyNames) {
    const ett = knToEtt[keyName];
    entities.push(isObject(ett) ? ett : null);
  }
  return entities;
};

const data = { updateTotal };

export default data;
