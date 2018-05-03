const {
  failure,
  success,
  payload,
  isFailure,
} = require('@pheasantplucker/failables')
const Datastore = require('@google-cloud/datastore')
const ramda = require('ramda')

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

const getDatastoreKeySymbol = () => {
  if (!datastore) {
    console.log('no data')
    const datastoreClient = createDatastoreClient()
    if (isFailure(datastoreClient)) {
      return failure(datastoreClient, {
        error: 'Couldnt make datastore client',
      })
    }
  }
  return success(datastore.KEY)
}

const makeDatastoreKey = (kind, entityName) => {
  if (kind === '' || entityName === '')
    return failure({ kind, entityName }, { error: 'Passed empty string' })
  try {
    const newKey = datastore.key([kind, entityName])
    return success(newKey)
  } catch (e) {
    return failure(e.toString())
  }
}

const readEntities = async keys => {
  const getEntities = await getRawEntitiesByKeys(keys)
  const rawResponse = payload(getEntities)
  return formatGetResponse(rawResponse)
}

const getRawEntitiesByKeys = async key => {
  try {
    const result = await datastore.get(key)
    if (result[0] === undefined) {
      return failure(key, {
        error:
          'Datastore returned undefined. Happens when key doesnt exist in DB.',
      })
    }
    const extraArrayLayersRemoved = [].concat.apply([], result)
    return success(extraArrayLayersRemoved)
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

const deleteByKey = async key => {
  try {
    const result = await datastore.delete(key)
    if (result[0].mutationResults) {
      return success(result)
    } else {
      return failure(result)
    }
  } catch (e) {
    return failure(e.toString())
  }
}

const deleteEntity = async entityOrKey => {
  try {
    if (datastore.isKey(entityOrKey)) {
      return await deleteByKey(entityOrKey)
    }

    if (entityOrKey.key) {
      const maybeKey = entityOrKey.key
      if (datastore.isKey(maybeKey)) {
        return await deleteByKey(maybeKey)
      }
    }

    return failure(entityOrKey, { error: `Couldnt find valid key.` })
  } catch (e) {
    return failure(e.toString())
  }
}

/* Helpers */
const formatGetResponse = response => {
  const symbol = payload(getDatastoreKeySymbol())
  const metadata = response.map(e => {
    return e[symbol]
  })
  const data = response.map(e => {
    const name = e[symbol].name
    delete e[symbol]
    return [name, e]
  })

  const objectifiedData = ramda.fromPairs(data)

  return success(objectifiedData, metadata)
}

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
  formatGetResponse,
}
