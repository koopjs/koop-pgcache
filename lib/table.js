var _ = require('lodash')
var Indexes = require('./indexes')
var Geohash = require('./geohash')

var Table = {}

// Note: set at runtime during db connection
Table.query = null
Table.log = null

/**
 * Creates a table and inserts features and metadat
 * creates indexes for each property in the features and substring indexes on geohashes
 *
 * @param {string} id - the dataset id to insert into
 * @param {Object} geojson - geojson features
 * @param {number} layerId - the layer id for this dataset
 * @param {function} callback - the callback when the query returns
 */
Table.createFeatureTable = function (id, geojson, layerId, callback) {
  var self = this

  var info = _.cloneDeep(geojson.info) || {}
  // DEPRECATED: to be removed in 2.0
  info.info = geojson.info

  info.name = geojson.name
  info.updated_at = geojson.updated_at
  info.expires_at = geojson.expires_at
  info.retrieved_at = geojson.retrieved_at
  info.status = geojson.status
  info.format = geojson.format
  info.sha = geojson.sha
  info.host = geojson.host

  var table = id + ':' + layerId
  var feature = (geojson.length) ? geojson[0].features[0] : geojson.features[0]

  var types = {
    'esriGeometryPolyline': 'Linestring',
    'esriGeometryPoint': 'Point',
    'esriGeometryPolygon': 'Polygon'
  }

  if (!feature) {
    feature = { geometry: { type: geojson.geomType || types[geojson.info.geometryType] } }
  }

  var indexes = Indexes.prepare(info)

  Table.create(table, buildSchemaFromFeature(feature), indexes, function (err) {
    if (err) {
      callback(err, false)
      return
    }

    // insert each feature
    if (geojson.length) {
      geojson = geojson[0]
    }

    // TODO why not use an update query here?
    self.query('delete from "' + self.infoTable + '" where id=\'' + table + ":info'", function (err, res) {
      if (err) console.log(err)
      self.query('insert into "' + self.infoTable + '" values (\'' + table + ":info','" + JSON.stringify(info).replace(/'/g, '') + "')", function (err, result) {
        if (!geojson.features.length) return callback(err, true)
        self.insertPartial(id, geojson, layerId, function (err) {
          callback(err, true)
        })
      })
    })
  })
}

/**
 * Creates a new table
 * checks to see if the table exists, create it if not
 *
 * @param {string} name - the name of the index
 * @param {string} schema - the schema to use for the table
 * @param {Array} indexes - an array of indexes to place on the table
 * @param {function} callback - the callback when the query returns
 * @private
 */
Table.create = function (name, schema, indexes, callback) {
  var self = this
  // set callback to noop if it hasn't been passed in
  callback = callback || function () {}
  var create = 'CREATE TABLE IF NOT EXISTS "' + name + '" ' + schema
  self.query(create, function (err, result) {
    if (err) return callback(new Error('Failed to create table ' + name + ' error:' + err))
    if (!indexes || !indexes.length) return callback()
    Indexes._add(name, indexes, function (err) {
      callback(err)
    })
  })
}

/**
 * Inserts an array of features
 * used as a way to insert pages of features, and only features, not metadata
 *
 * @param {string} id - the dataset id to insert into
 * @param {Object} geojson - geojson features
 * @param {number} layerId - the layer id for this dataset
 * @param {function} callback - the callback when the query returns
 */
Table.insertFeatures = function (id, geojson, layerId, callback) {
  var self = this
  var table = id + ':' + layerId
  var sql = 'BEGIN;INSERT INTO "' + table + '" (feature, geohash) VALUES '
  var preparedFeatures = geojson.features.map(prepareFeature)
  sql += preparedFeatures.join(',') + ';COMMIT;'
  this.query(sql, function (err, res) {
    if (err) {
      self.query('ROLLBACK;', function () {
        callback(err, false)
      })
    } else {
      callback(null, true)
    }
  })
}

/**
 * Creates the sql needed to insert the feature
 *
 * @param {string} table - the table to insert into
 * @param {Object} feature - a geojson feature
 * @private
 */
function prepareFeature (feature) {
  var featurestring = JSON.stringify(feature).replace(/'/g, '')

  if (feature.geometry && feature.geometry.coordinates && feature.geometry.coordinates.length) {
    var geohash = Geohash.create(feature, this.geohashPrecision)
    return "('" + featurestring + "', '" + geohash + "')"
  } else {
    return "('" + featurestring + "', null)"
  }
}

/**
 * Builds a table schema from a geojson feature
 * each schema in the db is essentially the same except for geometry type
 * which is based off the geometry of the feature passed in here
 *
 * @param {Object} feature - a geojson feature   * @returns {string} schema
 * @private
 */
function buildSchemaFromFeature (feature) {
  var schema = '('
  var props = ['id SERIAL PRIMARY KEY', 'feature JSON', 'geohash varchar(10)']
  schema += props.join(',') + ')'
  return schema
}

module.exports = Table
