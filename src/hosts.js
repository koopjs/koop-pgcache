'use strict'
const util = require('util')
const EventEmitter = require('events').EventEmitter

function Hosts (client, log) {
  this.client = client
  this.log = log
  this.table = 'koop:hosts'
}

util.inherits(Hosts, EventEmitter)

Hosts.prototype.insert = function (source, id, host, callback) {
  const sql = `INSERT INTO "${this.table}" VALUES ('${source}','${id}','${host}');`
  this.client.query(sql, callback)
}

Hosts.prototype.select = function (source, id, callback) {
  let sql
  if (typeof id === 'function') {
    callback = id
    sql = `SELECT host from "${this.table}" WHERE source='${source}';`
  } else {
    sql = `SELECT host from "${this.table}" WHERE source='${source}' AND id='${id}';`
  }
  this.client.query(sql, (err, result) => {
    if (err) return callback(err)
    const host = result.rows[0].host
    callback(null, host)
  })
}

Hosts.prototype.delete = function (source, id, callback) {
  const sql = `DELETE FROM "${this.table}" WHERE source='${source} AND id='${id}';`
  this.client.query(sql, callback)
}

module.exports = Hosts
