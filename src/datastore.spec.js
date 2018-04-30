const { assertSuccess, payload } = require(`@pheasantplucker/failables`)
const equal = require('assert').deepEqual
const { createDatastoreClient, makeDatastoreKey } = require('./datastore')
// const uuid = require('uuid')

const { GC_PROJECT_ID } = process.env

describe(`datastore.js`, () => {
  const kind = 'testKind'
  const entityName = 'testEntity'

  describe(`createDatastoreClient()`, () => {
    it(`should return a client`, () => {
      const result = createDatastoreClient(GC_PROJECT_ID)
      assertSuccess(result)
    })
  })

  describe('makeDatastoreKey()', () => {
    it('should return a valid key', () => {
      const result = makeDatastoreKey(kind, entityName)
      const key = payload(result)
      equal(key.name, entityName)
      equal(key.kind, kind)
      assertSuccess(result)
    })
  })
})
