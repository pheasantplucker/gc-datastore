const {
  failure,
  success,
  payload,
  isFailure,
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

// want as:
// {
//   testEntity1:{description:'no where now here when ew'},
//   testEntity2:{description:'how now brown cow'}
// }

const getRawEntitiesByKeys = async key => {
  try {
    const result = await datastore.get(key)
    if (result[0] === undefined) {
      return failure(key, {
        error:
          'Datastore returned undefined. Happens when key doesnt exist in DB.',
      })
    }
    return success(result)
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

const formatGetResponse = response => {
  const symbol = payload(getDatastoreKeySymbol())
  const metadata = response.map(e => {
    return e[symbol]
  })
  const data = response.map(e => {
    const name = e[symbol].name
    delete e[symbol]
    return { [name]: e }
  })
  return success(data[0], metadata[0])
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

    return failure(entityOrKey, { error: `Couldn't find valid key.` })
  } catch (e) {
    return failure(e.toString())
  }
}

module.exports = {
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
