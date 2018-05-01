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
} = require('./datastore')
// const uuid = require('uuid')

const { GC_PROJECT_ID } = process.env

describe(`datastore.js`, () => {
  const kind = 'testKind'
  const entityName1 = 'testEntity1'
  const entityName2 = 'testEntity2'
  const testData1 = { description: 'no where now here wh' }
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

  describe('writeEntity()', () => {
    it('should write an entity to Datastore', async () => {
      const entity = payload(makeEntityByName(kind, entityName1, testData1))
      const result = await writeEntity(entity)
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

    it.skip('should return both entities if given an array', async () => {
      const keys = [testKey1, testKey2]
      const result = await getEntitiesByKeys(keys)
      assertSuccess(result)
      console.log(`two keys:`, payload(result))
      const keysData = payload(result)
      const entity1 = keysData[0]
      const entity2 = keysData[1]
      equal(entity1, testData1)
      equal(entity2, testData2)
    })
  })
})
