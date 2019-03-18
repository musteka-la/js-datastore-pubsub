'use strict'

const { Key } = require('interface-datastore')
const { keyToTopic, topicToKey, keyToStoreKey } = require('./utils')

const errcode = require('err-code')
const assert = require('assert')
const debug = require('debug')
const log = debug('datastore-pubsub:publisher')
log.error = debug('datastore-pubsub:publisher:error')

// DatastorePubsub is responsible for providing an api for pubsub to be used as a datastore with
// [TieredDatastore]{@link https://github.com/ipfs/js-datastore-core/blob/master/src/tiered.js}
class DatastorePubsub {
  /**
   * Creates an instance of DatastorePubsub.
   * @param {*} pubsub - pubsub implementation.
   * @param {*} datastore - datastore instance.
   * @param {*} peerId - peer-id instance.
   * @param {Object} validator - validator functions.
   * @param {function(entry, peerId, callback)} validator.validate - function to validate a entry.
   * @param {function(received, current, callback)} validator.select - function to select the newest between two entries.
   * @param {function(key, callback)} subscriptionKeyFn - optional function to manipulate the key topic received before processing it.
   * @param {Object} keyEncoders - optional object to aid in encoding and decoding keys, with the following properties
   * @params {function(key)} keyEncoders.keyToTopic - convert a key to a topic
   * @params {function(topic)} keyEncoders.topicToKey - convert a topic to a key
   * @params {function(key)} keyEncoders.keyToStoreKey - convert a key to a datastore key
   * @memberof DatastorePubsub
   */
  constructor (pubsub, datastore, peerId, validator, subscriptionKeyFn, keyEncoders = {}) {
    assert.strictEqual(typeof validator, 'object', 'missing validator')
    assert.strictEqual(typeof validator.validate, 'function', 'missing validate function')
    assert.strictEqual(typeof validator.select, 'function', 'missing select function')
    subscriptionKeyFn && assert.strictEqual(typeof subscriptionKeyFn, 'function', 'invalid subscriptionKeyFn received')
    keyEncoders.keyToTopic && assert.strictEqual(typeof keyEncoders.keyToTopic, 'function', 'invalid keyEncoders.keyToTopic received')
    keyEncoders.topicToKey && assert.strictEqual(typeof keyEncoders.topicToKey, 'function', 'invalid topicToKey received')
    keyEncoders.keyToStoreKey && assert.strictEqual(typeof keyEncoders.keyToStoreKey, 'function', 'invalid keyToStoreKey received')

    this._pubsub = pubsub
    this._datastore = datastore
    this._peerId = peerId
    this._validator = validator
    this._handleSubscriptionKeyFn = subscriptionKeyFn

    this._keyToTopicFn = keyEncoders.keyToTopic || keyToTopic
    this._topicToKeyFn = keyEncoders.topicToKey || topicToKey
    this._keyToStoreKeyFn = keyEncoders.keyToStoreKey || keyToStoreKey

    // Bind _onMessage function, which is called by pubsub.
    this._onMessage = this._onMessage.bind(this)
  }

  /**
   * Publishes a value through pubsub.
   * @param {Buffer} key identifier of the value to be published.
   * @param {Buffer} val value to be propagated.
   * @param {function(Error)} callback
   * @returns {void}
   */
  put (key, val, callback) {
    if (!Buffer.isBuffer(key)) {
      const errMsg = `datastore key does not have a valid format`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_DATASTORE_KEY'))
    }

    if (!Buffer.isBuffer(val)) {
      const errMsg = `received value is not a buffer`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_VALUE_RECEIVED'))
    }

    const stringifiedTopic = this._keyToTopicFn(key)

    log(`publish value for topic ${stringifiedTopic}`)

    // need to subscribe to the topic to store on the local store
    this._subscribe(stringifiedTopic, (err) => {
      if (err) {
        return callback(err)
      }

      // Publish entry to pubsub
      this._pubsub.publish(stringifiedTopic, val, callback)
    })
  }

  /**
   * Try to subscribe a topic with Pubsub and returns the local value if available.
   * @param {Buffer} key identifier of the value to be subscribed.
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   */
  get (key, callback) {
    if (!Buffer.isBuffer(key)) {
      const errMsg = `datastore key does not have a valid format`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_DATASTORE_KEY'))
    }

    const stringifiedTopic = this._keyToTopicFn(key)
    this.isSubscribed(stringifiedTopic, (err, subscribed) => {
      if (err) {
        return callback(err)
      }

      if (subscribed) {
        return this._getLocal(key, callback)
      }

      this._subscribe(stringifiedTopic, (err) => {
        if (err) {
          return callback(err)
        }

        this._getLocal(key, callback)
      })
    })
  }

  isSubscribed (stringifiedTopic, callback) {
    this._pubsub.ls((err, res) => {
      if (err) {
        return callback(err)
      }

      // If already subscribed, just try to get it
      if (res && Array.isArray(res) && res.indexOf(stringifiedTopic) > -1) {
        return callback(null, true)
      }

      callback(null, false)
    })
  }

  _subscribe (stringifiedTopic, callback) {
    // Subscribe
    this._pubsub.subscribe(stringifiedTopic, this._onMessage, (err) => {
      if (err) {
        const errMsg = `cannot subscribe topic ${stringifiedTopic}`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_SUBSCRIBING_TOPIC'))
      }
      log(`subscribed values for key ${stringifiedTopic}`)

      callback()
    })
  }

  /**
   * Unsubscribe topic.
   * @param {Buffer} key identifier of the value to unsubscribe.
   * @returns {void}
   */
  unsubscribe (key) {
    const stringifiedTopic = this._keyToTopicFn(key)

    this._pubsub.unsubscribe(stringifiedTopic, this._onMessage)
  }

  // Get entry from local datastore
  _getLocal (key, callback) {
    const storeKey = this._keyToStoreKeyFn(key)

    this._datastore.get(storeKey, (err, dsVal) => {
      if (err) {
        if (err.code !== 'ERR_NOT_FOUND') {
          const errMsg = `unexpected error getting the record for ${storeKey.toString()}`

          log.error(errMsg)
          return callback(errcode(new Error(errMsg), 'ERR_UNEXPECTED_ERROR_GETTING_RECORD'))
        }
        const errMsg = `local entry requested was not found for ${storeKey.toString()}`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_NOT_FOUND'))
      }

      if (!Buffer.isBuffer(dsVal)) {
        const errMsg = `found entry that we couldn't convert to a value`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_INVALID_RECORD_RECEIVED'))
      }

      callback(null, dsVal)
    })
  }

  // handles pubsub subscription messages
  _onMessage (msg) {
    const { data, topicIDs } = msg
    let key
    try {
      key = this._topicToKeyFn(topicIDs[0])
    } catch (err) {
      log.error(err)
      return
    }

    log(`message received for ${key} topic`)

    if (this._handleSubscriptionKeyFn) {
      this._handleSubscriptionKeyFn(key, (err, res) => {
        if (err) {
          log.error('message discarded by the subscriptionKeyFn')
          return
        }

        this._storeIfSubscriptionIsBetter(res, data)
      })
    } else {
      this._storeIfSubscriptionIsBetter(key, data)
    }
  }

  // Store the received record if it is better than the current stored
  _storeIfSubscriptionIsBetter (key, data) {
    this._isBetter(key, data, (err, res) => {
      if (!err && res) {
        this._storeRecord(Buffer.from(key), data)
      }
    })
  }

  // Validate record according to the received validation function
  _validateRecord (value, peerId, callback) {
    this._validator.validate(value, peerId, callback)
  }

  // Select the best record according to the received select function.
  _selectRecord (receivedRecord, currentRecord, callback) {
    this._validator.select(receivedRecord, currentRecord, (err, res) => {
      if (err) {
        log.error(err)
        return callback(err)
      }

      // If the selected was the first (0), it should be stored (true)
      callback(null, res === 0)
    })
  }

  // Verify if the record received through pubsub is valid and better than the one currently stored
  _isBetter (key, val, callback) {
    // validate received record
    this._validateRecord(val, key, (err, valid) => {
      // If not valid, it is not better than the one currently available
      if (err || !valid) {
        const errMsg = 'record received through pubsub is not valid'

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_NOT_VALID_RECORD'))
      }

      // Get Local record
      const dsKey = new Key(key)

      this._getLocal(dsKey.toBuffer(), (err, currentRecord) => {
        // if the old one is invalid, the new one is *always* better
        if (err) {
          return callback(null, true)
        }

        // if the same record, do not need to store
        if (currentRecord.equals(val)) {
          return callback(null, false)
        }

        // verify if the received record should replace the current one
        this._selectRecord(val, currentRecord, callback)
      })
    })
  }

  // add record to datastore
  _storeRecord (key, data) {
    const storeKey = this._keyToStoreKeyFn(key)

    this._datastore.put(storeKey, data, (err) => {
      if (err) {
        log.error(`record for ${key.toString()} could not be stored in the routing`)
        return
      }

      log(`record for ${key.toString()} was stored in the datastore`)
    })
  }

  open (callback) {
    const errMsg = `open function was not implemented yet`

    log.error(errMsg)
    return callback(errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET'))
  }

  has (key, callback) {
    const errMsg = `has function was not implemented yet`

    log.error(errMsg)
    return callback(errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET'))
  }

  delete (key, callback) {
    const errMsg = `delete function was not implemented yet`

    log.error(errMsg)
    return callback(errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET'))
  }

  close (callback) {
    const errMsg = `close function was not implemented yet`

    log.error(errMsg)
    return callback(errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET'))
  }

  batch () {
    const errMsg = `batch function was not implemented yet`

    log.error(errMsg)
    throw errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET')
  }

  query () {
    const errMsg = `query function was not implemented yet`

    log.error(errMsg)
    throw errcode(new Error(errMsg), 'ERR_NOT_IMPLEMENTED_YET')
  }
}

exports = module.exports = DatastorePubsub
