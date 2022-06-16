'use strict'
const {
  AbstractLevel,
  AbstractIterator,
  AbstractKeyIterator,
  AbstractValueIterator
} = require('abstract-level')

const Redis = require('ioredis')
const { parseURL } = require('ioredis/built/utils')

const ModuleError = require('module-error')
const crypto = require('crypto')
const { connect } = require('./scripts-loader')

const rangeOptions = new Set(['gt', 'gte', 'lt', 'lte'])
const kClient = Symbol('client')
const kExec = Symbol('exec')
const kBuffer = Symbol('buffered')
const kAdvance = Symbol('advance')
const kAdvanceV = Symbol('advance=many')
const kNext = Symbol('next')
const kReverse = Symbol('reverse')
const kCursor = Symbol('pointer')
const kDone = Symbol('done')
const kStart = Symbol('start')
const kEnd = Symbol('end')
const kOptions = Symbol('options')
const kFetch = Symbol('fetch')
const kExecRange = Symbol('exec-range')
const kHighWaterMark = Symbol('highWaterMark')
const kValueEncoding = Symbol('valueEncoding')
const kInit = Symbol('init')
const kLocation = Symbol('location')
const kHashId = Symbol('hashId')
const kQuitDBOnClose = Symbol('quitOnClose')

const defaultHighWaterMark = 128

class RedisIterator extends AbstractIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  async [kExecRange] (encoding, count) {
    const prefix = this[kLocation]
    const zKey = prefix + ':z'
    const start = this[kStart]
    const end = this[kEnd]
    const hKey = prefix + ':h'
    const client = this[kClient]
    const method = 'iterPairs' + (isBufferEncoding(encoding) ? 'Buffer' : '')

    const reply = await client[method](zKey, hKey, this[kReverse] ? 1 : 0, start, end, count)
    const values = new Array(reply.length / 2)
    let k = 0
    for (let i = 0; i < reply.length; i += 2) {
      const v = reply[i + 1]
      values[k++] = [reply[i], v === null ? undefined : v]
    }
    this[kStart] = concatKey('(', reply[reply.length - 2])
    return values
  }

  /**
   * Retrieve the next item from iterator buffer, fetching from the server if needed
   * @param callback function
   */
  _next (callback) {
    this[kNext]((err, val) => {
      if (err) return this.nextTick(callback, err)
      // val at this point is an array[2]
      this.nextTick(callback, null, val[0], val[1])
    })
  }
}

class RedisKeyIterator extends AbstractKeyIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  // get next batch of results from redis
  async [kExecRange] (encoding, count) {
    const client = this[kClient]
    const prefix = this[kLocation]
    const zKey = prefix + ':z'
    const start = this[kStart]
    const end = this[kEnd]
    const suffix = isBufferEncoding(encoding) ? 'Buffer' : ''

    // key streams: no need for lua
    const method = (this[kReverse] ? 'zrevrangebylex' : 'zrangebylex') + suffix
    const reply = await client[method](zKey, start, end, 'limit', 0, count)
    this[kStart] = concatKey('(', reply[reply.length - 1])
    return reply
  }

  _next (callback) {
    this[kNext]((err, val) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, val)
    })
  }
}

class RedisValueIterator extends AbstractValueIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  async [kExecRange] (encoding, count) {
    const prefix = this[kLocation]
    const zKey = prefix + ':z'
    const start = this[kStart]
    const end = this[kEnd]
    const hKey = prefix + ':h'
    const client = this[kClient]
    const method = 'iterValues' + (isBufferEncoding(encoding) ? 'Buffer' : '')

    const reply = await client[method](zKey, hKey, this[kReverse] ? 1 : 0, start, end, count)
    const values = new Array(reply.length)
    for (let i = 0; i < reply.length; i++) {
      const v = reply[i]
      values[i] = v === null ? undefined : v
    }
    this[kStart] = concatKey('(', reply.pop())
    return values
  }

  _next (callback) {
    this[kNext]((err, val) => {
      if (err) return this.nextTick(callback, err)
      // val at this point is an array[2]
      this.nextTick(callback, null, val)
    })
  }
}

for (const Ctor of [RedisIterator, RedisKeyIterator, RedisValueIterator]) {
  Ctor.prototype[kInit] = function (db, options) {
    this[kReverse] = !!options.reverse
    this[kLocation] = db[kLocation]
    this[kClient] = db[kClient]
    this[kCursor] = 0
    this[kDone] = false
    this[kBuffer] = []
    this[kHighWaterMark] = options.highWaterMark || defaultHighWaterMark
    this[kOptions] = options
    this[kValueEncoding] = options.valueEncoding // ????

    const { start, end } = parseRangeOptions(options)
    this[kStart] = start
    this[kEnd] = end
  }

  Ctor.prototype[kAdvance] = function () {
    const buf = this[kBuffer]
    let cursor = this[kCursor]
    if (cursor < buf.length) {
      const val = buf[cursor++]
      this[kCursor] = cursor
      return val
    } else {
      return undefined
    }
  }

  /**
   * Fetch the next size items
   * @param size
   * @param callback
   */
  Ctor.prototype[kAdvanceV] = function (size, options, callback) {
    const encoding = options.valueEncoding || this[kValueEncoding]
    const entries = []

    const loop = () => {
      const buffer = this[kBuffer]
      const cursor = this[kCursor]
      if (buffer.length && cursor < buffer.length) {
        const wanted = size - entries.length
        const part = buffer.slice(cursor, cursor + wanted - 1)
        entries.push(...part)
        this[kCursor] += part.length
      }
      if (entries.length < size && !this[kDone]) {
        this[kFetch](encoding, (err) => {
          if (err) return this.nextTick(callback, err)
          this.nextTick(loop)
        })
      } else {
        return this.nextTick(callback, null, entries)
      }
    }

    this.nextTick(loop)
  }

  /**
   * Retrieve the next item from iterator buffer and fetch from server if needed
   * @param callback function
   */
  Ctor.prototype[kNext] = function (callback) {
    const complete = () => {
      const val = this[kAdvance]()
      return this.nextTick(callback, null, val)
    }

    // fast path
    if (this[kCursor] < this[kBuffer].length) {
      complete()
    } else if (this[kDone]) {
      this.nextTick(callback)
    } else {
      const encoding = this[kValueEncoding]
      this[kFetch](encoding, (err) => {
        if (err) return this.nextTick(callback, err)
        complete()
      })
    }
  }

  Ctor.prototype._nextv = function (size, options, callback) {
    this[kAdvanceV](size, options, (err, entries) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, entries)
    })
  }

  Ctor.prototype._all = function (options, callback) {
    const res = []
    const BatchSize = 500 // todo: config

    const loop = () => {
      this[kAdvanceV](BatchSize, options, (err, entries) => {
        if (err) return this.nextTick(callback, err)

        res.push(...entries)
        if (entries.length < BatchSize) {
          this.nextTick(callback, null, res)
        } else {
          this.nextTick(loop)
        }
      })
    }

    // Some time to breathe
    this.nextTick(loop)
  }

  Ctor.prototype._seek = function (target, options) {
    // todo: do we need to respect previous bounds set on the iterator ?
    const opts = {
      reverse: this[kReverse],
      gt: target
    }
    const { start, end } = parseRangeOptions(opts)
    this[kStart] = start
    this[kEnd] = end
    this[kCursor] = 0
    this[kBuffer] = []
    this[kDone] = false
  }

  /**
   * Gets a batch of key or values or pairs from redis
   */
  Ctor.prototype[kFetch] = function (encoding, callback) {
    if (this[kDone] || this.db.status === 'closed') {
      this[kDone] = true
      return this.nextTick(callback)
    }
    this[kBuffer] = []
    this[kCursor] = 0

    const highWaterMark = this[kHighWaterMark]
    let size
    if (this.limit > -1) {
      const remain = this.limit - this.count
      if (remain <= 0) {
        this[kDone] = true
        return this.nextTick(callback)
      }
      size = remain <= highWaterMark ? remain : highWaterMark
    } else {
      size = highWaterMark
    }

    this[kExecRange](encoding, size).then(values => {
      this[kBuffer] = values
      this[kDone] = values.length === 0
      this.nextTick(callback)
    }).catch(err => this.nextTick(callback, err))
  }
}

/**
 * Map of connections to the RedisLevel instances sharing it
 * the key is a hash of the connection options
 * @type {Map<string, { client: Redis; levels: RedisLevel[]; }>}
 */
const connections = new Map()

class RedisLevel extends AbstractLevel {
  constructor (location, options, _) {
    let forward

    if (typeof location === 'object' && location !== null) {
      const { location: _location, ...forwardOptions } = location
      location = _location
      forward = forwardOptions
    } else {
      forward = options
    }

    if (typeof location !== 'string' || location === '') {
      throw new TypeError("The first argument 'location' must be a non-empty string")
    }

    super({
      seek: true,
      permanence: false,
      createIfMissing: false,
      errorIfExists: false,
      encodings: { buffer: true, utf8: true, view: true }
    }, forward)

    this[kLocation] = sanitizeLocation(location)
  }

  get location () {
    return this[kLocation]
  }

  /**
   * @param options OpenOptions  either one of
   *  - ioredis instance.
   *  - object with { connection: Redis }
   *  - object with { connection: redis_url }
   *  - object with { connection: { port: portNumber, host: host } ... other options passed to ioredis }
   *
   * When a client is created it is reused across instances of
   * RedisLevel unless the option `ownClient` is truthy.
   * For a client to be reused, it requires the same port, host and options.
   */
  _open (options, callback) {
    // todo: do we need to handle `passive` ?
    const opts = Object.assign({
      connection: {
        port: 6379,
        host: '127.0.0.1',
        db: 0
      }
    }, options)
    this[kHighWaterMark] = options.highWaterMark || defaultHighWaterMark
    if (typeof opts.connection === 'string') {
      opts.connection = parseURL(opts.connection)
    }
    const location = this.location
    const redis = opts.connection

    if (redis && typeof redis.hget === 'function') {
      this[kClient] = redis
      this[kQuitDBOnClose] = false
    } else if (!options.ownClient) {
      const hashOptions = _getBaseOptions(location, redis)
      const hashId = this[kHashId] = hash(hashOptions)
      const dbDesc = connections.get(hashId)
      if (dbDesc) {
        this[kClient] = dbDesc.client
        dbDesc.levels.push(this)
      }
    } else {
      this[kQuitDBOnClose] = true
    }

    let isNew = false
    let client = this[kClient]
    if (!client) {
      client = new Redis(redis)
      isNew = true
      this[kClient] = client
      if (!options.ownClient) {
        connections.set(this[kHashId], { client, levels: [this] })
      }
    }

    connect(client).then(() => {
      if (options.clearOnOpen === true && isNew) {
        this.destroy(false, callback)
      } else {
        this.nextTick(callback)
      }
    }).catch(callback)
  }

  _close (callback) {
    const quitOnClose = this[kQuitDBOnClose]
    if (quitOnClose === false) {
      return this.nextTick(callback)
    }
    let client
    if (quitOnClose !== true) {
      // close the client only if it is not used by others:
      const hashId = this[kHashId]
      const dbDesc = connections.get(hashId)
      if (dbDesc) {
        client = dbDesc.client
        dbDesc.levels = dbDesc.levels.filter(x => x !== this)
        if (dbDesc.levels.length !== 0) {
          // still used by another RedisLevel
          return this.nextTick(callback)
        }
        connections.delete(hashId)
      }
    } else {
      client = this[kClient]
    }
    if (client) {
      try {
        client.disconnect()
      } catch (x) {
        console.log('Error attempting to close the redis client', x)
      }
    }
    this.nextTick(callback)
  }

  _put (key, value, options, callback) {
    if (value === undefined || value === null) {
      value = ''
    }
    this[kExec](appendPutCmd([], key, value, this.location), callback)
  }

  _get (key, options, callback) {
    const complete = (err, value) => {
      if (err) {
        return this.nextTick(callback, err)
      }

      // redis returns null for non-existent keys
      if (value === null) {
        return this.nextTick(callback, new ModuleError('NotFound', { code: 'LEVEL_NOT_FOUND' }))
      }

      this.nextTick(callback, null, value)
    }

    const client = this[kClient]
    const rkey = this.location + ':h'

    if (isBufferEncoding(options.valueEncoding)) {
      client.hgetBuffer(rkey, key, complete)
    } else {
      client.hget(rkey, key, complete)
    }
  }

  _getMany (keys, options, callback) {
    const client = this[kClient]
    const rkey = this.location + ':h'

    const complete = (err, values) => {
      if (err) {
        return this.nextTick(callback, err)
      }

      const result = new Array(keys.length)
      for (let i = 0; i < keys.length; i++) {
        const value = values[i]
        result[i] = (value === null) ? undefined : value
      }
      this.nextTick(callback, null, result)
    }

    if (isBufferEncoding(options.valueEncoding)) {
      client.hmgetBuffer(rkey, keys, complete)
    } else {
      client.hmget(rkey, keys, complete)
    }
  }

  _del (key, options, callback) {
    this[kExec](appendDelCmd([], key, this.location), callback)
  }

  _batch (operationArray, options, callback) {
    const commandList = []
    const prefix = this.location
    for (let i = 0; i < operationArray.length; i++) {
      const operation = operationArray[i]
      if (operation.type === 'put') {
        appendPutCmd(commandList, operation.key, operation.value, prefix)
      } else if (operation.type === 'del') {
        appendDelCmd(commandList, operation.key, prefix)
      } else {
        const error = new ModuleError('Unknown type of operation ' + JSON.stringify(operation), { code: 'LEVEL_NOT_SUPPORTED' })
        return this.nextTick(callback, error)
      }
    }
    this[kExec](commandList, callback)
  }

  _clear (options, callback) {
    const commandList = []
    if (options.limit === -1 && !Object.keys(options).some(isRangeOption)) {
      commandList.push(['del', this.location + ':h'])
      commandList.push(['del', this.location + ':z'])
      // Delete everything.
      return this[kExec](commandList, callback)
    }

    const batchSize = options.batchSize || this.db[kHighWaterMark]
    if (Number.isNaN(batchSize)) {
      const error = new TypeError('batchSize must be a number')
      return this.nextTick(callback, error)
    }

    const iterator = this._keys({ ...options })
    let limit = options.limit < 0 ? Infinity : options.limit

    function closeIterator (err) {
      return iterator.close((e) => {
        // we'll ignore any iterator closing errors
        this.nextTick(callback, err)
      })
    }

    const loop = () => {
      iterator.nextv(batchSize, options, (err, values) => {
        if (err) {
          return closeIterator(err)
        }
        if (values.length === 0) {
          return closeIterator()
        }
        values.forEach(key => {
          appendDelCmd(commandList, key, this.location)
        })
        limit = limit - values.length
        this[kExec](commandList, (err) => {
          if (err) {
            return closeIterator(err)
          }
          if (limit > 0 && !iterator[kDone]) {
            return this.nextTick(loop)
          }

          return closeIterator()
        })
      })
    }

    this.nextTick(loop)
  }

  _iterator (options) {
    return new RedisIterator(this, options)
  }

  _keys (options) {
    return new RedisKeyIterator(this, options)
  }

  _values (options) {
    return new RedisValueIterator(this, options)
  }

  async destroy (doClose, callback) {
    if (!callback && typeof doClose === 'function') {
      callback = doClose
      doClose = true
    }

    const client = this[kClient]
    try {
      await client.del(this.location + ':h', this.location + ':z')
      if (doClose) {
        await this.close()
      }
      if (callback) {
        this.nextTick(callback)
      }
    } catch (err) {
      if (callback) {
        return this.nextTick(callback, err)
      }
      throw err
    }
  }

  // for testing
  get client () {
    return this[kClient]
  }

  static reset (callback) {
    connections.forEach(v => {
      try {
        v.client.disconnect()
      } catch (x) {
      }
    })
    connections.clear()
    if (callback) {
      return process.nextTick(callback)
    }
  }

  static get connectionCount () {
    return connections.size
  }
}

RedisLevel.prototype[kExec] = function (commandList, callback) {
  this[kClient].multi(commandList).exec(callback)
}

exports.RedisLevel = RedisLevel

function isRangeOption (k) {
  return rangeOptions.has(k)
}

function parseRangeOptions (options) {
  const reverse = !!options.reverse
  let start, end
  let exclusiveStart = false
  let exclusiveEnd = false

  if (options.gt !== undefined) {
    if (!reverse) {
      exclusiveStart = true
      start = options.gt
    } else {
      exclusiveEnd = true
      end = options.gt
    }
  } else if (options.gte !== undefined) {
    if (!reverse) {
      start = options.gte
    } else {
      end = options.gte
    }
  }
  if (options.lt !== undefined) {
    if (!reverse) {
      exclusiveEnd = true
      end = options.lt
    } else {
      exclusiveStart = true
      start = options.lt
    }
  } else if (options.lte !== undefined) {
    if (!reverse) {
      end = options.lte
    } else {
      start = options.lte
    }
  }

  start = !start ? (reverse ? '+' : '-') : (concatKey((exclusiveStart ? '(' : '['), start))
  end = !end ? (reverse ? '-' : '+') : (concatKey((exclusiveEnd ? '(' : '['), end))

  return { start, end }
}

function appendPutCmd (commandList, key, value, prefix) {
  commandList.push(['hset', prefix + ':h', key, value === undefined ? '' : value])
  commandList.push(['zadd', prefix + ':z', 0, key])
  return commandList
}

function appendDelCmd (commandList, key, prefix) {
  commandList.push(['hdel', prefix + ':h', key])
  commandList.push(['zrem', prefix + ':z', key])
  return commandList
}

/**
 * Internal: generate the options for redis.
 * create an identifier for a redis client from the options passed to _open.
 * when the identifier is identical, it is safe to reuse the same client.
 */
function _getBaseOptions (location, options) {
  const redisIdOptions = [
    'host', 'port', 'db', 'family', 'keyPrefix', 'tls', 'dropBufferSupport',
    'enableOfflineQueue', 'path', 'connectTimeout', 'reconnectOnError'
  ]
  let opts = { }
  if (typeof options === 'string') {
    const parsed = parseURL(options.connection)
    if (parsed.auth) {
      parsed.auth_pass = parsed.auth.split(':')[1]
    }
    opts = { ...parsed }
  } else {
    opts = { ...options }
  }

  const redisOptions = {}
  redisIdOptions.forEach(function (opt) {
    if (opts[opt] !== undefined && opts[opt] !== null) {
      redisOptions[opt] = opts[opt]
    }
  })

  return redisOptions
}

function sanitizeLocation (location) {
  if (!location) {
    return 'rl'
  }
  if (location.indexOf('://') > 0) {
    const url = parseURL(location)
    location = url.hostname || 'rd'
  }
  if (location.charAt(0) === '/') {
    return location.substring(1)
  }
  // Keep the hash delimited by curly brackets safe
  // as it is used by redis-cluster to force the selection of a slot.
  if (location.indexOf('%7B') === 0 && location.indexOf('%7D') > 0) {
    location = location.replace('%7B', '{').replace('%7D', '}')
  }
  return location
}

function isBufferEncoding (encoding) {
  return ['buffer', 'view', 'binary'].indexOf(encoding) !== -1
}

function hash (data) {
  const source = objToString(data)
  return crypto.createHash('sha256').update(source).digest('hex')
}

function objToString (hash) {
  if (typeof hash !== 'object') {
    return '' + hash
  }
  const keys = Object.keys(hash).sort()
  const parts = keys.map((key) => {
    const val = hash[key]
    return key + ':' + objToString(val)
  })
  return parts.join('')
}

function concatKey (prefix, key, force) {
  if (typeof key === 'string' && (force || key.length)) {
    return prefix + key
  }
  if (Buffer.isBuffer(key) && (force || key.length)) {
    return Buffer.concat([Buffer.from(prefix), key])
  }
  return key
}
