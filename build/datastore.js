'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

const {
  failure,
  success,
  payload,
  isFailure,
  meta
} = require('@pheasantplucker/failables-node6');
const Datastore = require('@google-cloud/datastore');
const ramda = require('ramda');

let datastore;

const createDatastoreClient = projectId => {
  try {
    const newDatastore = new Datastore({
      projectId: projectId
    });
    datastore = newDatastore;
    return success(newDatastore);
  } catch (e) {
    return failure(e.toString());
  }
};

const getDatastoreKeySymbol = () => {
  return success(datastore.KEY);
};

const makeDatastoreKey = (kind, entityName) => {
  if (kind === '' || entityName === '') return failure({ kind, entityName }, { error: 'Passed empty string' });
  try {
    const newKey = datastore.key([kind, entityName]);
    return success(newKey);
  } catch (e) {
    return failure(e.toString());
  }
};

const readEntities = (() => {
  var _ref = _asyncToGenerator(function* (keys) {
    const getEntities = yield getRawEntitiesByKeys(keys);
    const rawResponse = payload(getEntities);
    return formatResponse(rawResponse);
  });

  return function readEntities(_x) {
    return _ref.apply(this, arguments);
  };
})();

const getRawEntitiesByKeys = (() => {
  var _ref2 = _asyncToGenerator(function* (key) {
    try {
      const result = yield datastore.get(key);
      if (result[0] === undefined) {
        return failure(key, {
          error: 'Datastore returned undefined. Happens when key doesnt exist in DB.'
        });
      }
      const extraArrayLayersRemoved = [].concat.apply([], result);
      return success(extraArrayLayersRemoved);
    } catch (e) {
      return failure(e.toString());
    }
  });

  return function getRawEntitiesByKeys(_x2) {
    return _ref2.apply(this, arguments);
  };
})();

const makeEntityByName = (kind, entityName, data) => {
  const dsKey = payload(makeDatastoreKey(kind, entityName));
  const entity = {
    key: dsKey,
    data: data
  };
  return success(entity);
};

const writeEntity = (() => {
  var _ref3 = _asyncToGenerator(function* (entity) {
    const datastoreInitialized = checkIfDatastoreInitialized();
    if (isFailure(datastoreInitialized)) return datastoreInitialized;

    try {
      const result = yield datastore.save(entity);
      if (result[0].mutationResults) {
        return success(result);
      } else {
        return failure(result);
      }
    } catch (e) {
      return failure(e.toString());
    }
  });

  return function writeEntity(_x3) {
    return _ref3.apply(this, arguments);
  };
})();

const deleteByKey = (() => {
  var _ref4 = _asyncToGenerator(function* (key) {
    try {
      const result = yield datastore.delete(key);
      if (result[0].mutationResults) {
        return success(result);
      } else {
        return failure(result);
      }
    } catch (e) {
      return failure(e.toString());
    }
  });

  return function deleteByKey(_x4) {
    return _ref4.apply(this, arguments);
  };
})();

const deleteEntity = (() => {
  var _ref5 = _asyncToGenerator(function* (entityOrKey) {
    try {
      if (datastore.isKey(entityOrKey)) {
        return yield deleteByKey(entityOrKey);
      }

      if (entityOrKey.key) {
        const maybeKey = entityOrKey.key;
        if (datastore.isKey(maybeKey)) {
          return yield deleteByKey(maybeKey);
        }
      }

      return failure(entityOrKey, { error: `Couldnt find valid key.` });
    } catch (e) {
      return failure(e.toString());
    }
  });

  return function deleteEntity(_x5) {
    return _ref5.apply(this, arguments);
  };
})();

const createQueryObj = (kind, ...rest) => {
  if (typeof kind != 'string' || kind === '') {
    return failure([kind, ...rest], { error: 'NEed a kind.' });
  }
  const thisQueryObj = datastore.createQuery(...rest, kind);
  return success(thisQueryObj);
};

const runQuery = (() => {
  var _ref6 = _asyncToGenerator(function* (queryObj) {
    try {
      const runningQuery = yield datastore.runQuery(queryObj);
      const entities = runningQuery[0];
      const queryEndDetails = runningQuery[1];
      const formatted = formatResponse(entities);
      if (isFailure(formatted)) return formatted;
      const formattedEntities = payload(formatted);
      const formattedKeys = meta(formatted);

      //test something?
      return success(formattedEntities, { queryEndDetails, formattedKeys });
    } catch (e) {
      return failure(e.toString());
    }
  });

  return function runQuery(_x6) {
    return _ref6.apply(this, arguments);
  };
})();

const runQueryKeysOnly = (() => {
  var _ref7 = _asyncToGenerator(function* (queryObj) {
    try {
      const keysOnlyQuery = queryObj.select('__key__');
      const runningQuery = yield datastore.runQuery(keysOnlyQuery);
      const entities = runningQuery[0];
      const queryEndDetails = runningQuery[1];
      const formatted = formatKeyResponse(entities);
      if (isFailure(formatted)) return formatted;
      const formattedEntities = payload(formatted);
      const formattedKeys = meta(formatted);

      //test something?
      return success(formattedEntities, { queryEndDetails, formattedKeys });
    } catch (e) {
      return failure(e.toString());
    }
    return success();
  });

  return function runQueryKeysOnly(_x7) {
    return _ref7.apply(this, arguments);
  };
})();

/* Helpers */
const formatResponse = response => {
  const symbol = payload(getDatastoreKeySymbol());
  const metadata = response.map(e => {
    return e[symbol];
  });
  const data = response.map(e => {
    const name = e[symbol].name;
    delete e[symbol];
    return [name, e];
  });

  const objectifiedData = ramda.fromPairs(data);

  return success(objectifiedData, metadata);
};

const formatKeyResponse = response => {
  const symbol = payload(getDatastoreKeySymbol());
  const metadata = response.map(e => {
    return e[symbol];
  });
  const data = response.map(e => {
    const name = e[symbol].name;
    return [name, e[symbol]];
  });

  const objectifiedData = ramda.fromPairs(data);

  return success(objectifiedData, metadata);
};

const checkIfDatastoreInitialized = caller => {
  if (datastore) return success();
  return failure(caller, {
    error: 'DatastoreClient was called but not Created!'
  });
};

module.exports = {
  readEntities,
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
  getRawEntitiesByKeys,
  deleteEntity,
  deleteByKey,
  getDatastoreKeySymbol,
  formatResponse,
  formatKeyResponse,
  createQueryObj,
  runQuery,
  runQueryKeysOnly
};