const {
  failure,
  success,
  // isFailure,
  payload,
} = require('@pheasantplucker/failables')
const Datastore = require('@google-cloud/datastore')

let datastore

const createDatastoreClient = projectId => {
  try {
    const newDatastore = new Datastore({
      projectId: projectId,
    })
    datastore = newDatastore
    return success(newDatastore)
  } catch (e) {
    return failure(e.toString())
  }
}

const makeDatastoreKey = (kind, entityName) => {
  try {
    const newKey = datastore.key([kind, entityName])
    return success(newKey)
  } catch (e) {
    return failure(e.toString())
  }
}

const makeEntityByName = (kind, entityName, data) => {
  const dsKey = payload(makeDatastoreKey(kind, entityName))
  const entity = {
    key: dsKey,
    data: data,
  }
  return success(entity)
}

const writeEntity = async entity => {
  try {
    return success(await datastore.save(entity))
  } catch (e) {
    return failure(e.toString())
  }
}

module.exports = {
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
}
