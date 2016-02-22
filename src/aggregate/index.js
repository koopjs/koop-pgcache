'use strict'
const util = require('util')
const EventEmitter = require('events').EventEmitter
const Statistics = require('./statistics')
const Geoservices = require('../geoservices')
const Geohash = require('./geohash')

function Aggregate (client, log) {
  this.log = log
  this.client = client
  this.geohash = new Geohash(client, log)
}

/**
 * Gets the count of all features in a table
 *
 * @param {string} table - the table name
 * @param {Object} options - optional params from the query string: where, geometry, order_by
 * @param {function} callback - returns the count
 */
Aggregate.prototype.count = function (table, options, callback) {
  const filter = Geoservices.parse(options)
  const sql = `SELECT count(*) AS count from "${table}" ${filter}`
  this.client.query(sql, (err, result) => {
    if (err) return callback(err)
    callback(null, parseInt(result.rows[0].count, 10))
  })
}

/**
 * Gets a set of statistics from the DB
 * Supports where and geometry filters and group by
 * @param {string} table to get data from
 * @param {object} statistics - the stat type: min, max, avg, count, var, stddev
 * @param {Object} options - optional params for the query: where, geometry, groupBy
 * @param {function} callback - when the query is done
 */
Aggregate.prototype.statistics = function (table, statistics, options, callback) {
  const sql = Statistics.generateSql(table, statistics, options)
  this.client.query(sql, (err, result) => {
    if (err) return callback(err)
    /* process result */
  })
}

/**
 * Gets the extent of all features in a table
 *
 * @param {string} table - the table name
 * @param {Object} options - optional params from the querystring like where and geometry
 * @param {function} callback - returns the count
 */
Aggregate.prototype.extent = function (table, options, callback) {
  const filter = Geoservices.parse(options)
  const sql = `SELECT ST_AsGeoJSON(ST_Extent(st_geomfromgeojson(feature->>'geometry'))) as extent FROM "${table}" ${filter}`
  this.client.query(sql, (err, result) => {
    if (err) return callback(err)
    let bbox
    try {
      bbox = JSON.parse(result.rows[0].extent).coordinates
    } catch (e) {
      return callback(e)
    }
    const extent = {
      xmin: bbox[0][0][0],
      ymin: bbox[0][0][1],
      xmax: bbox[0][2][0],
      ymax: bbox[0][2][1],
      spatialReference: {
        wkid: 4326,
        latestWkid: 4326
      }
    }
    callback(null, extent)
  })
}

Aggregate.prototype.geohash = function (table, options, callback) {
  options.limit = options.limit || 2000
  options.precision = 8 || options.precision
  this.geohash.aggregate(table, options, (err, geohash) => {
    callback(err, geohash)
  })
}

util.inherits(Aggregate, EventEmitter)

module.exports = Aggregate
