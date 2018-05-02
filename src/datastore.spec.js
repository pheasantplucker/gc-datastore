const {
  assertSuccess,
  assertFailure,
  payload,
} = require(`@pheasantplucker/failables`)
const equal = require('assert').deepEqual
const {
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
  getEntitiesByKeys,
  deleteEntity,
  deleteByKey,
} = require('./datastore')
// const uuid = require('uuid')

const { GC_PROJECT_ID } = process.env

describe(`datastore.js`, () => {
  const kind = 'testKind'
  const entityName1 = 'testEntity1'
  const entityName2 = 'testEntity2'
  const nonexistantEntityName = 'ERROR_ERROR_IF_THIS_IS_WRITEN_BAD'
  const testData1 = { description: 'no where now here when ew' }
  const testData2 = { description: 'how now brown cow' }

  describe(`createDatastoreClient()`, () => {
    it(`should return a client`, () => {
      const result = createDatastoreClient(GC_PROJECT_ID)
      assertSuccess(result)
    })
  })

  createDatastoreClient(GC_PROJECT_ID)
  const testKey1 = payload(makeDatastoreKey(kind, entityName1))
  const testKey2 = payload(makeDatastoreKey(kind, entityName2))
  const nonexistantKey = payload(makeDatastoreKey(kind, nonexistantEntityName))

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

  describe(`deleteEntity()`, () => {
    it.skip(`should remove an entity from the DB`, async () => {
      const entity1 = payload(makeEntityByName(kind, entityName1, testData1))
      const result = await deleteEntity(entity1)
      console.log(`result:`, result)
      assertSuccess(result)
    })
  })

  describe(`deleteByKey()`, () => {
    it(`should remove an entity from the DB by key`, async () => {
      const entity1 = payload(makeEntityByName(kind, entityName1, testData1))
      const writeResult = await writeEntity(entity1)
      assertSuccess(writeResult)
      const result = await deleteByKey(testKey1)
      assertSuccess(result)
      const entity1Read = await getEntitiesByKeys(testKey1)
      assertFailure(entity1Read)
    })
  })

  describe('writeEntity()', () => {
    it('should write an entity to Datastore', async () => {
      const entity = payload(makeEntityByName(kind, entityName1, testData1))
      const result = await writeEntity(entity)
      deleteEntity(entity)
      assertSuccess(result)
    })

    it('should fail with no entity', async () => {
      const result = await writeEntity()
      assertFailure(result)
    })
  })

  describe('getEntitiesByKeys()', () => {
    it('should return the correct entity', async () => {
      const result = await getEntitiesByKeys(testKey1)
      assertSuccess(result)
      const keyData = payload(result)
      equal(keyData[0], testData1)
    })

    it('should fail with an entity not in the DS', async () => {
      const result = await getEntitiesByKeys(nonexistantKey)
      assertFailure(result)
    })

    it.skip('should return both entities if given an array', async () => {
      const keys = [testKey1, testKey2]
      const entity2 = payload(makeEntityByName(kind, entityName2, testData2))
      writeEntity(entity2)

      const result = await getEntitiesByKeys(keys)
      assertSuccess(result)
      const keysData = payload(result)
      console.log(keysData)
      const entity1Data = keysData[0]
      const entity2Data = keysData[1]
      equal(entity1Data, testData1)
      equal(entity2Data, testData2)
    })
  })
})
