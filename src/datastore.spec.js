const { assertSuccess, payload } = require(`@pheasantplucker/failables`)
const equal = require('assert').deepEqual
const {
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
} = require('./datastore')
// const uuid = require('uuid')

const { GC_PROJECT_ID } = process.env

describe(`datastore.js`, () => {
  const kind = 'testKind'
  const entityName = 'testEntity'
  const testData = { description: 'no where now here' }

  describe(`createDatastoreClient()`, () => {
    it(`should return a client`, () => {
      const result = createDatastoreClient(GC_PROJECT_ID)
      assertSuccess(result)
    })
  })

  describe('makeDatastoreKey()', () => {
    it('should return a valid key', () => {
      const result = makeDatastoreKey(kind, entityName)
      assertSuccess(result)

      const key = payload(result)
      equal(key.name, entityName)
      equal(key.kind, kind)
    })
  })

  describe('makeEntityByName()', () => {
    it('should make an entity to execute', () => {
      const result = makeEntityByName(kind, entityName, testData)
      assertSuccess(result)

      const entity = payload(result)
      equal(entity.key, payload(makeDatastoreKey(kind, entityName)))
      equal(entity.data, testData)
    })
  })

  describe('writeEntity()', () => {
    it('should write an entity to Datastore', async () => {
      const entity = payload(makeEntityByName(kind, entityName, testData))
      const result = await writeEntity(entity)
      assertSuccess(result)
    })
  })
})
