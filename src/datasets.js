'use strict'
const util = require('util')
const EventEmitter = require('events').EventEmitter

function Datasets (client, log) {
  this.log = log
  this.client = client
}

util.inherits(Datasets, EventEmitter)

Datasets.prototype.drop = function (name, callback) {
  const sql = `DROP TABLE IF EXISTS ${name}`
  this.client.query(sql, (err, res) => {
    if (err) return callback(err)
    this.emit('drop', name)
  })
}

Datasets.prototype.promote = function (from, to, callback) {
  const sql = `DROP TABLE IF EXISTS ${to}; ALTER TABLE ${from} RENAME TO ${to};`
  this.client.query(sql, (err, res) => {
    if (err) return callback(err)
    this.emit('promotion', {from, to})
  })
}

Datasets.prototype.addIndexes = function (name, options, callback) {}

/**
 * Creates a new table
 * checks to see if the table exists, create it if not
 *
 * @param {string} name - the name of the index
 * @param {function} callback - the callback when the query returns
 */
Datasets.prototype.create = function (name, options, callback) {
  const schema = buildSchema(options.geomType)
  const sql = `CREATE TABLE IF NOT EXISTS "${name}" ${schema};`
  this.client.query(sql, (err, result) => {
    this.emit('create', {name, options})
    callback(err, result)
  })
}

/**
 * Builds a table schema from a geojson feature
 * each schema in the db is essentially the same except for geometry type
 * which is based off the geometry of the feature passed in here
 *
 * @param {Object} feature - a geojson feature   * @returns {string} schema
 * @private
 */
function buildSchema (geomType) {
  let schema = '('
  const columns = ['id SERIAL PRIMARY KEY', 'feature JSONB', 'geohash varchar(10)']
  schema += columns.join(',') + ')'
  return schema
}

module.exports = Datasets
