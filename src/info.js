const util = require('util')
const EventEmitter = require('events').EventEmitter

function Info (client, log) {
  this.client = client
  this.log = log
	this.infoTable = 'koop:info'
}

util.inherits(Info, EventEmitter)

Info.prototype.insert = function (id, info, callback) {
  // escape all single quotes
  const json = stringifyInfo(info)
  const sql = `INSERT INTO "${this.infoTable}" VALUES ('${id}','${json}')`
  this.client.query(sql, callback)
}

Info.prototype.select = function (id, callback) {
  const sql = `SELECT info FROM "${this.infoTable}" WHERE id='${id}'`
  this.client.query(sql, (err, result) => {
    if (err || !result || !result.rows || !result.rows.length) return callback(new Error('Resource not found'))
    const info = result.rows[0].info
    callback(null, info)
  })
}

Info.prototype.update = function (id, update, callback) {
  // if this is running against postgres 9.5+ we could do a straight update
  this.select(id, (err, existing) => {
    if (err) return callback(err)
    const updated = Object.assign(existing, update)
    const info = stringifyInfo(updated)
    const sql = `UPDATE "${this.infoTable}" SET info='${info}' WHERE id='${id}'`
    this.client.query(sql, callback)
  })
}

Info.prototype.delete = function (id, callback) {
  const sql = `DELETE FROM "${this.infoTable}" WHERE id='${id}'`
  this.client.query(sql, callback)
}

function stringifyInfo (info) {
  return JSON.stringify(info).replace(/'/g, '\'')
}

module.exports = Info
