var async = require('async')

var Indexes = {}
/**
 * Add indexes to a given table
 *
 * @param {string} table - the name of the table to add indexes to
 * @param {Object} options - determines what kind of indexes to add
 * @param {function} calls back after the indexes are added
 */
Indexes.add = function (table, options, callback) {
  var info = {}
  if (options.fields) {
    info.fields = options.fields
    info._indexFields = true
  }
  if (options.geohash) info._indexGeohash = true
  if (options.geometry) info._indexGeometry = true
  var indexes = Indexes.prepare(info)
  Indexes._add(table, indexes, callback)
}

/**
 * Prepares a set of indexes to be added by default
 *
 * @param {object} info - an info object that contains settings for indexing
 * @return {array} an array of index objects
 */
Indexes.prepare = function (info) {
  // in 2.0 we can pass in an option to decide whether to add indexes at insert time, for now we will just use info
  // default to true so there is no breaking change
  if (typeof info._indexGeohash === 'undefined') info._indexGeohash = true
  if (typeof info._indexFields === 'undefined') info._indexFields = true
  if (typeof info._indexGeometry === 'undefined') info._indexGeometry = true
  // a list of indexes to create on the new table
  var indexes = []
  if (info._indexGeohash) indexes = indexes.concat(geohashIndexes)
  if (info._indexGeometry) indexes.push(geometryIndex)
  if (info && info.fields && info._indexFields) indexes = indexes.concat(prepareFieldindexes(info.fields))
  return indexes
}

/**
 * Adds a pre-specified set of indexes to a table
 *
 * @param {string}  table - The table to add indexes to
 * @param {array} indexes - A set of index objects to be added to the table
 * @param {function} callback - calls back after the indexes are added
 */
Indexes._add = function (table, indexes, callback) {
  var indexName = table.replace(/:|-/g, '')
  async.each(indexes, function (index, done) {
    createIndex(table, indexName + '_' + index.name, index.using, function (err) {
      done(err)
    })
  }, function (error) {
    callback(error)
  })
}

/**
 * Creates an index on a given table
 * @param {string} table - the table to index
 * @param {string} name - the name of the index
 * @param {string} using - the actual field and type of the index
 * @param {function} callback - the callback when the query returns
 * @private
 */
function createIndex (table, name, using, callback) {
  var sql = 'CREATE INDEX ' + name + ' ON "' + table + '" USING ' + using
  Indexes.query(sql, function (err) {
    if (err) return callback(err)
    if (callback) callback()
  })
}

/**
 * Creates a set of field indexes
 *
 * @param {array} fields - the group of fields to prepare indexes for
 * @return {array} an array of field indexes
 * @private
 */
function prepareFieldindexes (fields) {
  var indexes
  fields.forEach(function (field) {
    var idx = {
      name: field,
      using: "btree ((feature->'properties'->>'" + field + "'))"
    }
    indexes.push(idx)
  })
  return indexes
}

/**
  * Default specification for a geometry index
  */
var geometryIndex = {
  name: 'gix',
  using: "GIST (ST_GeomfromGeoJSON(feature->>'geometry'))"
}

/**
 * Default specification for geohash indexes
 */
var geohashIndexes = [{
  name: 'substr3',
  using: 'btree (substring(geohash,0,3))'
}, {
  name: 'substr4',
  using: 'btree (substring(geohash,0,4))'
}, {
  name: 'substr5',
  using: 'btree (substring(geohash,0,5))'
}, {
  name: 'substr6',
  using: 'btree (substring(geohash,0,6))'
}, {
  name: 'substr7',
  using: 'btree (substring(geohash,0,7))'
}, {
  name: 'substr8',
  using: 'btree (geohash)'
}]

module.exports = Indexes
