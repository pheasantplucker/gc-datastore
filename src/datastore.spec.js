const {
  assertSuccess,
  assertFailure,
  payload,
  isSuccess,
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
  const entity1 = payload(makeEntityByName(kind, entityName1, testData1))

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

  describe(`deleteEntity()`, async () => {
    it(`should remove an entity from the DB`, async () => {
      const result = await deleteEntity(entity1)
      assertSuccess(result)
    })
  })

  describe(`deleteByKey()`, async () => {
    it(`should remove an entity from the DB by key`, async () => {
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
      const result = await writeEntity(entity1)
      assertSuccess(result)
      deleteEntity(entity1)
    })

    it('should fail with no entity', async () => {
      const result = await writeEntity()
      assertFailure(result)
    })
  })

  describe('formatGetResponse', () => {
    it(`should convert the raw response to a cleaner struct`, async () => {
      const writeResult = await writeEntity(entity1)
      assertSuccess(writeResult)
      if (isSuccess(writeResult)) {
        const result = await getEntitiesByKeys(testKey1)
        assertSuccess(result)
        const key1Data = payload(result)
        equal(testData1, key1Data)
      }
    })
  })

  describe('getEntitiesByKeys()', () => {
    it('should return the correct entity', async () => {
      const writeResult = await writeEntity(entity1)
      assertSuccess(writeResult)
      if (isSuccess(writeResult)) {
        const result = await getEntitiesByKeys(testKey1)
        assertSuccess(result)
        const keyData = payload(result)
        equal(keyData, testData1)
        deleteEntity(entity1)
      }
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
      const keysData = payload(result)[0]
      console.log(`keysData:`, keysData)
      equal(keysData.includes(testData1), true)
      equal(keysData.includes(testData2), true)
      deleteEntity(entity2)
    })
  })
})
