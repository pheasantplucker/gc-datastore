'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

const {
  assertSuccess,
  assertFailure,
  payload,
  isSuccess,
  isFailure,
  meta
} = require(`@pheasantplucker/failables-node6`);
const assert = require('assert');
const equal = assert.deepEqual;
const {
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
  deleteEntity,
  readEntities,
  formatResponse,
  createQueryObj,
  runQuery,
  runQueryKeysOnly,
  deleteByKey,
  getRawEntitiesByKeys,
  formatKeyResponse,
  getDatastoreKeySymbol
} = require('./datastore');

const { GC_PROJECT_ID } = process.env;

describe(`datastore.js`, () => {
  const kind = 'testKind';
  const kind2 = 'testKind2';
  const namespace = 'testNamespace';
  const entityName1 = 'testEntity1';
  const entityName2 = 'testEntity2';
  const nonexistantEntityName = 'ERROR_ERROR_IF_THIS_IS_WRITEN_BAD';
  const testData1 = { description: 'no where now here when ew' };
  const testData2 = { description: 'how now brown cow' };

  describe(`createDatastoreClient()`, () => {
    it(`should return a client`, () => {
      const result = createDatastoreClient(GC_PROJECT_ID);
      assertSuccess(result);
    });
  });

  createDatastoreClient(GC_PROJECT_ID);

  const testKey1 = payload(makeDatastoreKey(kind, entityName1));
  const entity1 = payload(makeEntityByName(kind, entityName1, testData1));

  const testKey2 = payload(makeDatastoreKey(kind, entityName2));
  const entity2 = payload(makeEntityByName(kind, entityName2, testData2));

  const nonexistantKey = payload(makeDatastoreKey(kind, nonexistantEntityName));
  const separateKindEntity = payload(makeEntityByName(kind2, entityName1, testData1));
  const setup = (() => {
    var _ref = _asyncToGenerator(function* () {
      const writeResult = yield writeEntity([entity1, entity2, separateKindEntity]);
      assertSuccess(writeResult);
    });

    return function setup() {
      return _ref.apply(this, arguments);
    };
  })();

  setup();

  describe('makeDatastoreKey()', () => {
    it('should return a valid key', () => {
      const result = makeDatastoreKey(kind, entityName1);
      assertSuccess(result);

      const key = payload(result);
      equal(key.name, entityName1);
      equal(key.kind, kind);
    });

    it('should fail with missing data', () => {
      const result = makeDatastoreKey('', '');
      assertFailure(result);
      const resultKind = makeDatastoreKey(kind, '');
      assertFailure(resultKind);
      const resultName = makeDatastoreKey('', entityName1);
      assertFailure(resultName);
    });
  });

  describe('makeEntityByName()', () => {
    it('should make an entity to execute', () => {
      const result = makeEntityByName(kind, entityName1, testData1);
      assertSuccess(result);

      const entity = payload(result);
      equal(entity.key, testKey1);
      equal(entity.data, testData1);
    });
  });

  describe(`deleteEntity()`, _asyncToGenerator(function* () {
    it(`should remove an entity from the DB`, _asyncToGenerator(function* () {
      const entityNameDel = 'deleteTest';
      const deleteKey = payload(makeDatastoreKey(kind, entityNameDel));
      const entityRead = yield getRawEntitiesByKeys(deleteKey);
      const entityDel = payload(makeEntityByName(kind, entityNameDel, 'delete'));
      if (isFailure(entityRead)) {
        const writeResult = yield writeEntity(entityDel);
        assertSuccess(writeResult);
        const result = yield deleteEntity(entityDel);
        assertSuccess(result);
      } else {
        const result = yield deleteEntity(entityDel);
        assertSuccess(result);
      }
    }));
  }));

  describe(`deleteByKey()`, _asyncToGenerator(function* () {
    it(`should remove an entity from the DB by key`, _asyncToGenerator(function* () {
      const writeResult = yield writeEntity(entity1);
      assertSuccess(writeResult);
      const result = yield deleteByKey(testKey1);
      assertSuccess(result);
      const entity1Read = yield getRawEntitiesByKeys(testKey1);
      assertFailure(entity1Read);
    }));
  }));

  describe('writeEntity()', () => {
    it('should write an entity to Datastore', _asyncToGenerator(function* () {
      const result = yield writeEntity(entity1);
      assertSuccess(result);
      const entity1Read = yield getRawEntitiesByKeys(testKey1);
      assertSuccess(entity1Read);
    }));

    it('should fail with no entity', _asyncToGenerator(function* () {
      const result = yield writeEntity();
      assertFailure(result);
    }));
  });

  describe(`getDatastoreKeySymbol()`, () => {
    it(`should Create a client if none exists`, () => {
      const result = getDatastoreKeySymbol();
      assertSuccess(result);
    });
    it(`should return the symbol`, () => {
      const result = getDatastoreKeySymbol();
      assertSuccess(result);
      const key = payload(result);
      equal('symbol', typeof key);
    });
  });

  describe('formatResponse()', () => {
    it(`should convert the raw response to a cleaner struct`, _asyncToGenerator(function* () {
      const getResponse = yield getRawEntitiesByKeys(testKey1);
      assertSuccess(getResponse);
      const key1Data = payload(getResponse);
      const result = formatResponse(key1Data);
      const cleanReturn = payload(result);
      assertSuccess(result);
      equal({ [entityName1]: testData1 }, cleanReturn);
    }));

    it(`should return multiple entities as object attributes`, _asyncToGenerator(function* () {
      const getResponse = yield getRawEntitiesByKeys([testKey1, testKey2]);
      assertSuccess(getResponse);
      const keysData = payload(getResponse);
      const result = formatResponse(keysData);
      const cleanReturn = payload(result);
      assertSuccess(result);
      equal({
        [entityName1]: testData1,
        [entityName2]: testData2
      }, cleanReturn);
    }));
  });

  describe('formatKeyResponse()', () => {
    it(`should convert the raw response to a cleaner struct`, _asyncToGenerator(function* () {
      const getResponse = yield getRawEntitiesByKeys(testKey1);
      assertSuccess(getResponse);
      const key1Data = payload(getResponse);
      const result = formatKeyResponse(key1Data);
      const cleanReturn = payload(result);
      assertSuccess(result);
      equal({ [entityName1]: testKey1 }, cleanReturn);
    }));

    it(`should return multiple entities as object attributes`, _asyncToGenerator(function* () {
      const getResponse = yield getRawEntitiesByKeys([testKey1, testKey2]);
      assertSuccess(getResponse);
      const keysData = payload(getResponse);
      const result = formatResponse(keysData);
      const cleanReturn = payload(result);
      assertSuccess(result);
      equal({
        [entityName1]: testData1,
        [entityName2]: testData2
      }, cleanReturn);
    }));
  });

  describe(`createQueryObj()`, () => {
    it(`should return a query object`, () => {
      const result = createQueryObj(kind);
      assertSuccess(result);
      const queryObj = payload(result);
      equal(typeof queryObj, 'object');
    });

    it('should fail with no kind', () => {
      const result = createQueryObj('');
      assertFailure(result);
    });

    it(`should set the namespace if given 2 args`, () => {
      const result = createQueryObj(kind, namespace);
      assertSuccess(result);
      const queryObj = payload(result);
      equal(queryObj.namespace, namespace);
      equal(queryObj.kinds[0], kind);
    });

    // it.skip(`COULD also set the datastore object scope`, () => {
    //    //but I don't know why you need that!
    // })
  });

  describe(`runQuery()`, () => {
    it(`should fail with no query`, _asyncToGenerator(function* () {
      const result = yield runQuery();
      assertFailure(result);
    }));

    it(`should return all elements from a query`, _asyncToGenerator(function* () {
      const query = payload(createQueryObj(kind));
      const result = yield runQuery(query);
      assertSuccess(result);
      const responses = payload(result);
      const metadata = meta(result);
      assert(metadata.queryEndDetails.moreResults, 'This attribute should exist.');
      // not sure what other tests are creating/deleting objects
      equal(Object.keys(responses).length > 1, true);
    }));
  });

  describe(`runQueryKeysOnly`, () => {
    it(`should return only the keys which are cheaper`, _asyncToGenerator(function* () {
      const query = payload(createQueryObj(kind));
      const result = yield runQueryKeysOnly(query);
      assertSuccess(result);
      const keys = payload(result);
      assert(Object.keys(keys).length > 1);
    }));
  });

  describe('readEntities()', _asyncToGenerator(function* () {
    it(`should return a nice formatted list with response stuff in metadata`, _asyncToGenerator(function* () {
      const result = yield readEntities(testKey1);
      assertSuccess(result);
      const keyData = payload(result);
      equal({
        [entityName1]: testData1
      }, keyData);
    }));
  }));

  describe('getRawEntitiesByKeys()', () => {
    it('should return the correct entity', _asyncToGenerator(function* () {
      const writeResult = yield writeEntity(entity1);
      assertSuccess(writeResult);
      if (isSuccess(writeResult)) {
        const result = yield getRawEntitiesByKeys(testKey1);
        assertSuccess(result);
        const rawResponse = payload(result);
        const responseData = rawResponse[0];
        equal(responseData, testData1);
      }
    }));

    it('should fail with an entity not in the DS', _asyncToGenerator(function* () {
      const result = yield getRawEntitiesByKeys(nonexistantKey);
      assertFailure(result);
    }));

    it('should return both entities if given an array', _asyncToGenerator(function* () {
      const keys = [testKey1, testKey2];
      const result = yield getRawEntitiesByKeys(keys);
      assertSuccess(result);
      const keysData = payload(result);
      equal(keysData.length, keys.length);
    }));
  });
});