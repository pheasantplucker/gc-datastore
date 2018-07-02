const {
  failure,
  success,
  payload,
  isFailure,
  meta,
} = require('@pheasantplucker/failables')
const Datastore = require('@google-cloud/datastore')
const { writeFile, mkdirs } = require('fs-extra')
const ramda = require('ramda')
const set_diff = require('lodash.difference')
let datastore, datastoreV1
let PROJECT_ID

const createDatastoreClient = project_id => {
  try {
    PROJECT_ID = project_id
    if (!datastore) {
      datastore = new Datastore({
        projectId: PROJECT_ID,
      })
    }
    if (!datastoreV1) {
      datastoreV1 = new Datastore.v1.DatastoreClient({
        projectId: PROJECT_ID,
      })
    }
    return success(datastore)
  } catch (e) {
    return failure(e.toString())
  }
}

function make_entity(kind, id, data, meta) {
  const key = payload(makeDatastoreKey(kind, id))
  return Object.assign(
    {},
    {
      key,
      data,
    },
    meta
  )
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

const makeArray = input => {
  if (Array.isArray(input)) return input
  return [input]
}

const readEntities = async keys => {
  const keyArray = makeArray(keys)

  const getEntities = await getRawEntitiesByKeys(keyArray)
  if (isFailure(getEntities)) {
    return getEntities
  }
  const rawResponse = payload(getEntities)
  if (rawResponse.length === 0) {
    return failure(keys, {
      error: 'Key read returned empty, key probably not in DB',
    })
  }

  const cleanResponseResult = formatResponse(rawResponse)
  if (isFailure(cleanResponseResult)) {
    return cleanResponseResult
  }
  const cleanResponse = payload(cleanResponseResult)

  const inputKeys = keyArray.map(key => {
    return key.name
  })

  let missingKeysObj = {}
  inputKeys.map(keyName => {
    if (!cleanResponse[keyName]) {
      missingKeysObj[keyName] = undefined
    }
  })
  const combinedOutput = Object.assign(cleanResponse, missingKeysObj)
  return success(combinedOutput)
}

const getRawEntitiesByKeys = async keys => {
  try {
    const result = await datastore.get(keys)
    if (result[0] === undefined) {
      return failure(keys, {
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

const writeEntity = async (entity, onlyWriteTestsToLocalPath = '') => {
  if (onlyWriteTestsToLocalPath) {
    try {
      // write to local path
      await mkdirs(onlyWriteTestsToLocalPath)
      const fileName = entity.key.name
      const fullPath = onlyWriteTestsToLocalPath + fileName
      await writeFile(fullPath, JSON.stringify(entity, null, '\t'))
      return success(fullPath)
    } catch (e) {
      return failure(e.toString())
    }
  }

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

const convertKeyToV1 = key => {
  const v1Path = {
    path: [
      {
        kind: key.kind,
        name: key.name,
      },
    ],
  }
  return Object.assign({}, key, v1Path)
}

const lookup = async keys => {
  try {
    const v1Keys = await keys.map(key => {
      return convertKeyToV1(key)
    })

    let request = {
      projectId: PROJECT_ID,
      keys: v1Keys,
    }
    const lookupRes = await datastoreV1.lookup(request)
    const response = lookupRes[0]
    return success(response)
  } catch (e) {
    return failure(e.toString())
  }
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

const insert = async ents => {
  const items = makeArray(ents)
  let inserted = []
  let existed = []
  for (let i = 0; i < items.length; i++) {
    try {
      await datastore.insert(items[i])
      inserted.push(items[i].key.name)
    } catch (e) {
      if (e.code === 6) {
        existed.push(items[i].key.name)
      } else {
        return failure(e.toString())
      }
    }
  }
  return success({ inserted, existed })
}

// ========================================================
//
//                     BATCH_MODE
//
// ========================================================
function make_key(namespace, kind, id) {
  if (!namespace) return failure('make_key no namespace')
  if (!kind) return failure('make_key no kind')
  if (!id) return failure('make_key no id')
  return success(
    datastore.key({
      namespace,
      path: [kind, id],
    })
  )
}

function batch_make_keys(namespace, data) {
  const keys = []
  for (let i = 0; i < data.length; i++) {
    const r1 = make_key(namespace, data[i][0], data[i][1])
    if (isFailure(r1)) return r1
    const p = payload(r1)
    keys.push(p)
  }
  return success(keys)
}

function batch_make_entities(namespace, data, meta) {
  const entities = []
  for (let i = 0; i < data.length; i++) {
    const r1 = make_key(namespace, data[i][0], data[i][1])
    if (isFailure(r1)) return r1
    const key = payload(r1)
    entities.push(Object.assign({}, { key, data: data[i][2] }, meta))
  }
  return success(entities)
}

// keys_and_data looks like
// const data = [
//   ['test_kind', 'bg1', { hash: '1', title: 'one' }],
//   ['test_kind', 'bg2', { hash: '2', title: 'two' }],
//   ['test_kind', 'bg3', { hash: '3', title: 'thr' }],
// ]
async function batch_set(namespace, data, meta) {
  try {
    const r1 = batch_make_entities(namespace, data, meta)
    if (isFailure(r1)) return r1
    const p = payload(r1)
    return writeEntity(p)
  } catch (error) {
    return failure(error.toString())
  }
}

async function batch_get(namespace, data) {
  try {
    const ids_to_find = data.map(d => d[1])
    const r1 = batch_make_keys(namespace, data)
    if (isFailure(r1)) return r1
    const keys = payload(r1)
    const r2 = await datastore.get(keys)
    const entities = r2[0]
    const r3 = formatResponse(entities)
    if (isFailure(r3)) return r3
    const formatted = payload(r3)
    const found = Object.keys(formatted).sort()
    const missing = set_diff(ids_to_find, found)
    return success({
      items: formatted,
      found,
      missing,
    })
  } catch (error) {
    return failure(error.toString())
  }
}

async function batch_delete(namespace, data) {
  try {
    const r1 = batch_make_keys(namespace, data)
    if (isFailure(r1)) return r1
    const r2 = await datastore.delete(payload(r1))
    return success({ count: r2[0].indexUpdates })
  } catch (error) {
    return failure(error.toString())
  }
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
  makeArray,
  lookup,
  convertKeyToV1,
  make_entity,
  insert,
  batch_get,
  batch_set,
  batch_delete,
}
