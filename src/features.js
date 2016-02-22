'use strict'
const util = require('util')
const EventEmitter = require('events').EventEmitter
const Geoservices = require('./geoservices')
const Info = require('./info')
const Geohash = require('./aggregate/geohash')
const spawn = require('child_process').spawn
const _ = require('highland')

function Features (client, log) {
  this.log = log
  this.client = client
  this.info = new Info(client, log)
}

util.inherits(Features, EventEmitter)

/**
 * Get features out of the db
 *
 * @param {string} id - the dataset id to insert into
 * @param {Object} options - optional params used for filtering features (where, geometry, etc)
 * @param {function} callback - the callback when the query returns
 */
Features.prototype.select = function (table, query, callback) {
  // We get info first because we may need to know some things about the schema in order to construct a query
  this.info.select(table, (err, info) => {
    if (err) return callback(err)
    query.fields = info.fields
    query.table = table
    const select = Geoservices.makeSelect(query)
    this.client.query(select, (err, result) => {
      if (err) return callback(err)
      const features = result.rows.map(transform)
      const featureCollection = { type: 'FeatureCollection', features }
      callback(null, featureCollection)
    })
  })
}

/**
 * Transforms a row from the database into a geojson Feature
 *
 * @param {object} row - a row from the db
 * @return {object} a geojson feature
 */
function transform (row) {
  return {
    'type': 'Feature',
    'id': row.id,
    'geometry': row.geom,
    'properties': row.props
  }
}

/**
 * Inserts an array of features
 * used as a way to insert pages of features, and only features, not metadata
 *
 * @param {string} name - the name of the tablee to insert into
 * @param {Object} geojson - geojson features
 * @param {function} callback - the callback when the query returns
 */
Features.prototype.insert = function (table, geojson, callback) {
  let insert = `BEGIN;INSERT INTO "${table}" (feature, geohash) VALUES `
  const preparedFeatures = geojson.features.map(prepareFeature)
  insert += `${preparedFeatures.join(',')};COMMIT;`
  this.client.query(insert, callback)
}

/**
 * Creates the sql needed to insert the feature
 *
 * @param {string} table - the table to insert into
 * @param {Object} feature - a geojson feature
 * @private
 */
function prepareFeature (feature) {
  const featurestring = JSON.stringify(feature).replace(/'/g, '')

  if (feature.geometry && feature.geometry.coordinates && feature.geometry.coordinates.length) {
    const geohash = Geohash.create(feature, 8)
    return `('${featurestring}', '${geohash}')`
  } else {
    return `('${featurestring}', null)`
  }
}

/**
 * Creates a stream that exports geojson from the database as a string
 *
 * @param {string} table - the table to exports
 * @param {object} options - includes which rows to export
 */
Features.prototype.createStream = function (table, options) {
  const dbStream = createDbStream(this.client.connection, table, options)
  const jsonMode = options.json
  // rows coming from the DB are newline terminated, so we need to split and filter to get individual rows
  const outStream = _()
  outStream.disconnect = function () {
    console.log(dbStream)
    dbStream.disconnect()
    dbStream.kill('sigterm')
    console.log(dbStream)
  }
  const featureStream = _(dbStream.stdout)
  .split()
  .compact()
  // postgres does not properly ignore escaped double quotes
  // the PSQL command accounts for this
  // however this causes the entire feature to be wrapped in single quotes
  .map(r => {
    return r.slice(1, -1)
  })

  dbStream.on('error', function (err) {
    outStream.emit('error', err)
    outStream.destroy()
  })

  dbStream.on('exit', function (code) {
    if (code > 0) {
      outStream.emit('error', new Error('Export stream failed'))
      outStream.destroy()
    }
  })
  return jsonMode ? featureStream.map(JSON.parse).pipe(outStream) : featureStream.pipe(outStream)
}

/**
 * Creates the source db stream
 *
 * @param {string} conn - psql connection string
 * @param {object} options - which rows to select
 * @param {function} callback - calls back with an error or a stream from the db
 * @private
 */
function createDbStream (conn, table, options) {
  const where = Geoservices.parse(options)
  // csv and quote options are a hack to ensure the string comes out properly escaped
  const sql = 'copy (select feature from "' + table + '" ' + where + ') to stdout with (format csv, quote "\'");'
  const params = ['-c', sql, '-d', conn]
  console.log(params)
  return spawn('psql', params)
}

module.exports = Features
