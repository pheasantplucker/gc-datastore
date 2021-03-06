const {
  assertSuccess,
  assertFailure,
  payload,
  isSuccess,
  isFailure,
  meta,
} = require(`@pheasantplucker/failables`)
const assert = require('assert')
const equal = assert.deepEqual
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
  getDatastoreKeySymbol,
  makeArray,
  lookup,
  convertKeyToV1,
  make_entity,
  insert,
  batch_get,
  batch_set,
  batch_delete,
} = require('./datastore')
const uuid = require('uuid')

// *********************************************
//
//                    SETUP
//
// *********************************************

const { GC_PROJECT_ID } = process.env
const kind = 'testKind'
const kind2 = 'testKind2'
const namespace = 'testNamespace'
const entityName1 = 'testEntity1'
const entityName2 = 'testEntity2'
const nonexistantEntityName = 'ERROR_ERROR_IF_THIS_IS_WRITEN_BAD'
const testData1 = { description: 'no where now here when ew' }
const testData2 = { description: 'how now brown cow' }
const result = createDatastoreClient(GC_PROJECT_ID)
assertSuccess(result)
const testKey1 = payload(makeDatastoreKey(kind, entityName1))
const entity1 = payload(makeEntityByName(kind, entityName1, testData1))

const testKey2 = payload(makeDatastoreKey(kind, entityName2))
const entity2 = payload(makeEntityByName(kind, entityName2, testData2))

const nonexistantKey = payload(makeDatastoreKey(kind, nonexistantEntityName))
const separateKindEntity = payload(
  makeEntityByName(kind2, entityName1, testData1)
)

describe(`datastore.js`, function() {
  this.timeout(4 * 1000)
  describe('writeEntity()', () => {
    it('should write an entity', async () => {
      const writeResult = await writeEntity([
        entity1,
        entity2,
        separateKindEntity,
      ])
      assertSuccess(writeResult)
    })
  })

  describe('makeDatastoreKey()', () => {
    it('should return a valid key', () => {
      const result = makeDatastoreKey(kind, entityName1)
      assertSuccess(result)

      const key = payload(result)
      equal(key.name, entityName1)
      equal(key.kind, kind)
    })

    it('should fail with missing data', () => {
      const result = makeDatastoreKey('', '')
      assertFailure(result)
      const resultKind = makeDatastoreKey(kind, '')
      assertFailure(resultKind)
      const resultName = makeDatastoreKey('', entityName1)
      assertFailure(resultName)
    })
  })

  describe('makeEntityByName()', () => {
    it('should make an entity to execute', () => {
      const result = makeEntityByName(kind, entityName1, testData1)
      assertSuccess(result)

      const entity = payload(result)
      equal(entity.key, testKey1)
      equal(entity.data, testData1)
    })
  })

  describe(`deleteEntity()`, async () => {
    it(`should remove an entity from the DB`, async () => {
      const entityNameDel = 'deleteTest'
      const deleteKey = payload(makeDatastoreKey(kind, entityNameDel))
      const entityRead = await getRawEntitiesByKeys(deleteKey)
      const entityDel = payload(makeEntityByName(kind, entityNameDel, 'delete'))
      if (isFailure(entityRead)) {
        const writeResult = await writeEntity(entityDel)
        assertSuccess(writeResult)
        const result = await deleteEntity(entityDel)
        assertSuccess(result)
      } else {
        const result = await deleteEntity(entityDel)
        assertSuccess(result)
      }
    })
  })

  describe(`deleteByKey()`, async () => {
    it(`should remove an entity from the DB by key`, async () => {
      const writeResult = await writeEntity(entity1)
      assertSuccess(writeResult)
      const result = await deleteByKey(testKey1)
      assertSuccess(result)
      const entity1Read = await getRawEntitiesByKeys(testKey1)
      assertFailure(entity1Read)
    })
  })

  describe('writeEntity()', () => {
    it('should write an entity to Datastore', async () => {
      const result = await writeEntity(entity1)
      assertSuccess(result)
      const entity1Read = await getRawEntitiesByKeys(testKey1)
      assertSuccess(entity1Read)
    })

    it('should fail with no entity', async () => {
      const result = await writeEntity()
      assertFailure(result)
    })

    it('should be able to write test data to a local path', async () => {
      const localPath = './testdata/'
      const result = await writeEntity(entity1, localPath)
      assertSuccess(result)
      const entity1Read = await getRawEntitiesByKeys(testKey1)
      assertSuccess(entity1Read)
    })
  })

  describe(`getDatastoreKeySymbol()`, () => {
    it(`should Create a client if none exists`, () => {
      const result = getDatastoreKeySymbol()
      assertSuccess(result)
    })
    it(`should return the symbol`, () => {
      const result = getDatastoreKeySymbol()
      assertSuccess(result)
      const key = payload(result)
      equal('symbol', typeof key)
    })
  })

  describe('formatResponse()', () => {
    it(`should convert the raw response to a cleaner struct`, async () => {
      const getResponse = await getRawEntitiesByKeys(testKey1)
      assertSuccess(getResponse)
      const key1Data = payload(getResponse)
      const result = formatResponse(key1Data)
      const cleanReturn = payload(result)
      assertSuccess(result)
      equal({ [entityName1]: testData1 }, cleanReturn)
    })

    it(`should return multiple entities as object attributes`, async () => {
      const getResponse = await getRawEntitiesByKeys([testKey1, testKey2])
      assertSuccess(getResponse)
      const keysData = payload(getResponse)
      const result = formatResponse(keysData)
      const cleanReturn = payload(result)
      assertSuccess(result)
      equal(
        {
          [entityName1]: testData1,
          [entityName2]: testData2,
        },
        cleanReturn
      )
    })
  })

  describe('formatKeyResponse()', () => {
    it(`should convert the raw response to a cleaner struct`, async () => {
      const getResponse = await getRawEntitiesByKeys(testKey1)
      assertSuccess(getResponse)
      const key1Data = payload(getResponse)
      const result = formatKeyResponse(key1Data)
      const cleanReturn = payload(result)
      assertSuccess(result)
      equal({ [entityName1]: testKey1 }, cleanReturn)
    })

    it(`should return multiple entities as object attributes`, async () => {
      const getResponse = await getRawEntitiesByKeys([testKey1, testKey2])
      assertSuccess(getResponse)
      const keysData = payload(getResponse)
      const result = formatResponse(keysData)
      const cleanReturn = payload(result)
      assertSuccess(result)
      equal(
        {
          [entityName1]: testData1,
          [entityName2]: testData2,
        },
        cleanReturn
      )
    })
  })

  describe(`createQueryObj()`, () => {
    it(`should return a query object`, () => {
      const result = createQueryObj(kind)
      assertSuccess(result)
      const queryObj = payload(result)
      equal(typeof queryObj, 'object')
    })

    it('should fail with no kind', () => {
      const result = createQueryObj('')
      assertFailure(result)
    })

    it(`should set the namespace if given 2 args`, () => {
      const result = createQueryObj(kind, namespace)
      assertSuccess(result)
      const queryObj = payload(result)
      equal(queryObj.namespace, namespace)
      equal(queryObj.kinds[0], kind)
    })

    // it.skip(`COULD also set the datastore object scope`, () => {
    //    //but I don't know why you need that!
    // })
  })

  describe(`lookup()`, () => {
    it(`should return an array of objects found in the DB Keys removing non-written keys`, async () => {
      const testKeys = [testKey1, nonexistantKey]
      const result = await lookup(testKeys)
      assertSuccess(result)
      const returnKeys = payload(result)
      assert(returnKeys.found.length, 1)
      assert(returnKeys.missing.length, 1)
    })
  })

  describe(`convertKeyToV1()`, () => {
    it(`should replace path with the nested abomination of kind,name`, () => {
      const result = convertKeyToV1(testKey1)
      const correctPath = [{ kind: kind, name: entityName1 }]
      equal(result.path, correctPath)
    })
  })

  describe(`runQuery()`, () => {
    it(`should fail with no query`, async () => {
      const result = await runQuery()
      assertFailure(result)
    })

    it(`should return all elements from a query`, async () => {
      const query = payload(createQueryObj(kind))
      const result = await runQuery(query)
      assertSuccess(result)
      const responses = payload(result)
      const metadata = meta(result)
      assert(
        metadata.queryEndDetails.moreResults,
        'This attribute should exist.'
      )
      // not sure what other tests are creating/deleting objects
      equal(Object.keys(responses).length > 1, true)
    })
  })

  describe(`runQueryKeysOnly`, () => {
    it(`should return only the keys which are cheaper`, async () => {
      const query = payload(createQueryObj(kind))
      const result = await runQueryKeysOnly(query)
      assertSuccess(result)
      const keys = payload(result)
      assert(Object.keys(keys).length > 1)
    })
  })

  describe(`makeArray()`, () => {
    it(`should return a success array from an array or string or fail`, () => {
      const result = makeArray('fling')
      assert(typeof result, 'array')
    })
  })

  describe('readEntities()', async () => {
    it(`should return a nice formatted list with response stuff in metadata`, async () => {
      const result = await readEntities(testKey1)
      assertSuccess(result)
      const keyData = payload(result)
      equal(
        {
          [entityName1]: testData1,
        },
        keyData
      )
    })

    it(`should work with an array`, async () => {
      const testKeys = [testKey1, testKey2]
      const result = await readEntities(testKeys)
      assertSuccess(result)
      const keyData = payload(result)
      equal(testData1, keyData[entityName1])
      equal(testData2, keyData[entityName2])
    })

    it(`should return undefined if it doesn't read`, async () => {
      const testKeys = [testKey1, testKey2, nonexistantKey]
      const result = await readEntities(testKeys)
      assertSuccess(result)
      const keyData = payload(result)
      equal(testData1, keyData[entityName1])
      equal(testData2, keyData[entityName2])
      equal(undefined, keyData[nonexistantKey])
    })

    it('should fail if the key doesnt exist', async () => {
      const result = await readEntities([nonexistantKey])
      assertFailure(result)
    })
  })

  describe('getRawEntitiesByKeys()', () => {
    it('should return the correct entity', async () => {
      const writeResult = await writeEntity(entity1)
      assertSuccess(writeResult)
      if (isSuccess(writeResult)) {
        const result = await getRawEntitiesByKeys(testKey1)
        assertSuccess(result)
        const rawResponse = payload(result)
        const responseData = rawResponse[0]
        equal(responseData, testData1)
      }
    })

    it('should fail with an entity not in the DS', async () => {
      const result = await getRawEntitiesByKeys(nonexistantKey)
      assertFailure(result)
    })

    it('should return both entities if given an array', async () => {
      const keys = [testKey1, testKey2]
      const result = await getRawEntitiesByKeys(keys)
      assertSuccess(result)
      const keysData = payload(result)
      equal(keysData.length, keys.length)
    })
  })
  describe('insert', () => {
    it('should insert a list of entities', async () => {
      const kind = 'insert_kind'
      const name1 = uuid.v4()
      const name2 = uuid.v4()
      const e1 = make_entity(kind, name1, { foo: 1 })
      const e2 = make_entity(kind, name2, { foo: 2 })
      const expected = { inserted: [name2], existed: [name1] }
      const r1 = await writeEntity(e1)
      assertSuccess(r1)
      const r2 = await insert([e1, e2])
      assertSuccess(r2)
      const p = payload(r2)
      equal(p, expected)
    })
  })

  describe(`batch`, () => {
    it('make some data', async () => {
      const namespace = 'namespace1'
      const meta = {
        excludeFromIndexes: ['hash', 'title'],
        method: 'insert',
      }
      const keys_and_data = [
        ['test_kind', 'bg1', { hash: '1', title: 'one' }],
        ['test_kind', 'bg2', { hash: '2', title: 'two' }],
        ['test_kind', 'bg3', { hash: '3', title: 'thr' }],
      ]
      const result = await batch_set(namespace, keys_and_data, meta)
      //console.log(payload(result))
      assertSuccess(result)
    })
    it('get the data as a map', async () => {
      const namespace = 'namespace1'
      const keys = [
        ['test_kind', 'bg1'],
        ['test_kind', 'bg2'],
        ['test_kind', 'bg3'],
        ['test_kind', 'bg4'],
      ]
      const fields = ['hash']
      const result = await batch_get(namespace, keys, fields)
      assertSuccess(result)
      equal(payload(result), {
        items: {
          bg1: { hash: '1', title: 'one' },
          bg2: { hash: '2', title: 'two' },
          bg3: { hash: '3', title: 'thr' },
        },
        found: ['bg1', 'bg2', 'bg3'],
        missing: ['bg4'],
      })
    })
    it('try getting keys that are not in the database', async () => {
      const namespace = 'namespace1'
      const keys = [['test_kind', 'bg5'], ['test_kind', 'bg6']]
      const fields = ['hash']
      const result = await batch_get(namespace, keys, fields)
      assertSuccess(result)
      const p = payload(result)
      equal(p, {
        items: {},
        found: [],
        missing: ['bg5', 'bg6'],
      })
    })
    it('delete the data', async () => {
      const namespace = 'namespace1'
      const keys = [
        ['test_kind', 'bg1'],
        ['test_kind', 'bg2'],
        ['test_kind', 'bg3'],
        ['test_kind', 'bg4'],
      ]
      const result = await batch_delete(namespace, keys)
      assertSuccess(result)
      const p = payload(result)
      equal(p, { count: 3 })
    })
  })
})
