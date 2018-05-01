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
  if (kind === '' || entityName === '')
    return failure({ kind, entityName }, 'Passed empty string')
  try {
    const newKey = datastore.key([kind, entityName])
    return success(newKey)
  } catch (e) {
    return failure(e.toString())
  }
}

const getEntitiesByKeys = async key => {
  try {
    return success(await datastore.get(key))
  } catch (e) {
    return failure(e.toString())
  }
}
//
// const dbGet = keys => {
//   const dbkeys = keys.map(k => {
//     return datastore.key([k[0], k[1]])
//   })
//   return new Promise(function(resolve, reject) {
//     datastore.get(dbkeys, function(err, entity) {
//       if (err) {
//         reject(err)
//       } else {
//         resolve(entity)
//       }
//     })
//   })
// }

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
    const result = await datastore.save(entity)
    if (result[0].mutationResults) {
      return success(result)
    } else {
      return failure(result)
    }
  } catch (e) {
    return failure(e.toString())
  }
}

module.exports = {
  createDatastoreClient,
  makeDatastoreKey,
  makeEntityByName,
  writeEntity,
  getEntitiesByKeys,
}
