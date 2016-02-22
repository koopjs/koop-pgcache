'use strict'
const ngeohash = require('ngeohash')
const centroid = require('turf-centroid')
const Geoservices = require('../geoservices')

function Geohash (client, log) {
  this.client = client
  this.log = log
}
/**
 * Creates a geohash from a features
 * computes the centroid of lines and polygons
 *
 * @param {Object} feature - a geojson feature
 * @param {number} precision - the precision at which the geohash will be created
 * @returns {string} geohash
 */
Geohash.create = function (feature, precision) {
  if (!feature.geometry || !feature.geometry.coordinates) return null
  if (feature.geometry.type !== 'Point') {
    feature = centroid(feature)
  }
  const pnt = feature.geometry.coordinates
  return ngeohash.encode(pnt[1], pnt[0], precision)
}

/**
 * Get a geohash aggregation for a set of features in the db
 * this will auto-reduce the precision of the geohashes if the given
 * precision exceeds the given limit.
 *
 * @param {string} table - the table to query
 * @param {Object} options - optional params like where and geometry, limit and precision
 * @param {function} callback - the callback when the query returns
 */
Geohash.aggregate = function (table, options, callback) {
  const agg = {}

  reducePrecision(table, options.precision, options, options.limit, (err, newPrecision) => {
    if (err) return callback(err)

    let geoHashSelect

    if (newPrecision <= options.precision) {
      geoHashSelect = 'substring(geohash,0,' + (newPrecision) + ')'
    } else {
      geoHashSelect = 'geohash'
    }

    const filter = Geoservices.parse(options)

    let sql = 'SELECT count(id) as count, ' + geoHashSelect + ' as geohash from "' + table + '"' + filter

    sql += ' GROUP BY ' + geoHashSelect
    this.client.query(sql, function (err, res) {
      if (!err && res && res.rows.length) {
        res.rows.forEach(function (row) {
          agg[row.geohash] = row.count
        })
        callback(err, agg)
      } else {
        callback(err, res)
      }
    })
  })
}

// recursively get geohash counts until we have a precision
// that reutrns less than the row limit
// this will return the precision that will return the number
// of geohashes less than the limit
function reducePrecision (table, p, options, limit, callback) {
  countDistinctGeoHash(table, p, options, function (err, count) {
    if (parseInt(count, 0) > limit) {
      reducePrecision(table, p - 1, options, limit, callback)
    } else {
      callback(err, p)
    }
  })
}

/**
 * Get the count of distinct geohashes for a query
 *
 * @param {string} table - the table to query
 * @param {string} precision - the precision at which to extract the distinct geohash counts
 * @param {Object} options - optional params like where and geometry
 * @param {function} callback - the callback when the query returns
 */
function countDistinctGeoHash (table, precision, options, callback) {
  let countSql = 'select count(DISTINCT(substring(geohash,0,' + precision + '))) as count from "' + table + '"'

  // apply any filters to the sql
  if (options.whereFilter) {
    countSql += options.whereFilter
  }

  if (options.geomFilter) {
    countSql += ((options.whereFilter) ? ' AND ' : ' WHERE ') + options.geomFilter
  }

  Geohash.query(countSql, function (err, res) {
    if (err) return callback(err, null)
    callback(null, res.rows[0].count)
  })
}

module.exports = Geohash
