'use strict'
const util = require('util')
const EventEmitter = require('events').EventEmitter

function Wkt (client, log) {
  this.client = client
  this.log = log
}

util.inherits(Wkt, EventEmitter)

/**
 * Inserts a WKT into the spatial_ref_sys table
 *
 * @param {integer} srid - the Spatial Reference ID
 * @param {string} wkt - the well know ext for the spatial referfence system
 */
Wkt.prototype.insert = function (srid, wkt, callback) {
  var sql = `INSERT INTO spatial_ref_sys (srid, srtext) VALUES ('${srid}','${wkt}');`
  this.client.query(sql, function (err, result) {
    if (err) return callback(err)
    callback(null, result)
  })
}

/**
 * Gets a WKT from the spatial_ref_sys table
 *
 * @param {integer} srid - the Spatial Reference ID
 * @return {string} wkt - the well known text for the spatial reference system
 */
Wkt.prototype.select = function (srid, callback) {
  const sql = `SELECT srtext FROM spatial_ref_sys WHERE srid=${srid};`
  this.client.query(sql, function (err, result) {
    if (err) return callback(err)
    let wkt
    try {
      wkt = extractWKT(result)
    } catch (e) {
      return callback(e)
    }
    callback(null, wkt)
  })
}

/**
 * Wrap the return from getWKT so we can test the sql statement
 *
 * @param {object} result - response from the DB
 * @return {string} - the body of the first row of the results
 */
function extractWKT (result) {
  if (result.rows[0]) return result.rows[0].srtext
  else throw new Error('No WKT found')
}

module.exports = Wkt
