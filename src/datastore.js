const {
  failure,
  success,
  payload,
  isFailure,
  meta,
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
  return formatResponse(rawResponse)
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
  const datastoreInitialized = checkIfDatastoreInitialized()
  if (isFailure(datastoreInitialized)) return datastoreInitialized

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

const createQueryObj = (kind, ...rest) => {
  if (typeof kind != 'string' || kind === '') {
    return failure([kind, ...rest], { error: 'NEed a kind.' })
  }
  const thisQueryObj = datastore.createQuery(...rest, kind)
  return success(thisQueryObj)
}

const runQuery = async queryObj => {
  try {
    const runningQuery = await datastore.runQuery(queryObj)
    const entities = runningQuery[0]
    const queryEndDetails = runningQuery[1]
    const formatted = formatResponse(entities)
    if (isFailure(formatted)) return formatted
    const formattedEntities = payload(formatted)
    const formattedKeys = meta(formatted)

    //test something?
    return success(formattedEntities, { queryEndDetails, formattedKeys })
  } catch (e) {
    return failure(e.toString())
  }
}

const runQueryKeysOnly = async queryObj => {
  try {
    const keysOnlyQuery = queryObj.select('__key__')
    const runningQuery = await datastore.runQuery(keysOnlyQuery)
    const entities = runningQuery[0]
    const queryEndDetails = runningQuery[1]
    const formatted = formatKeyResponse(entities)
    if (isFailure(formatted)) return formatted
    const formattedEntities = payload(formatted)
    const formattedKeys = meta(formatted)

    //test something?
    return success(formattedEntities, { queryEndDetails, formattedKeys })
  } catch (e) {
    return failure(e.toString())
  }
  return success()
}

/* Helpers */
const formatResponse = response => {
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

const formatKeyResponse = response => {
  const symbol = payload(getDatastoreKeySymbol())
  const metadata = response.map(e => {
    return e[symbol]
  })
  const data = response.map(e => {
    const name = e[symbol].name
    return [name, e[symbol]]
  })

  const objectifiedData = ramda.fromPairs(data)

  return success(objectifiedData, metadata)
}

const checkIfDatastoreInitialized = caller => {
  if (datastore) return success()
  return failure(caller, {
    error: 'DatastoreClient was called but not Created!',
  })
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
  formatResponse,
  formatKeyResponse,
  createQueryObj,
  runQuery,
  runQueryKeysOnly,
}
