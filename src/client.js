const Pg = require('pg')
const util = require('util')
const EventEmitter = require('events').EventEmitter
const _ = require('highland')

/**
 * Wrap's PgClient so we can pass it around an control events
 */
function Client (connection, log) {
  this.log = log
  this.connection = connection
  const client = this._client = new Pg.Client(connection)
}

util.inherits(Client, EventEmitter)

/**
 * Executes SQL against the DB
 * uses connection pooling to connect and query
 *
 * @param {string} sql - the sql to run
 * @param {function} callback - the callback when db returns
 * @param {boolean} retried - whether this query is being retried after an aborted transaction
 */
Client.prototype.query = function (sql, callback, retried) {
  Pg.connect(this.connection, (error, pgClient, done) => {
    if (error) {
      this.log.error('error fetching client from pool', error)
      return callback(error)
    }
    this.log.debug(truncateSql(sql))
    pgClient.query(sql, (err, result) => {
      // this error occurs when we have an aborted transaction
      // we'll try to clear that transaction
      if (err) this.log.error(queryError(err))
      if (err && err.code === '25P02' && !retried) {
        pgClient.query('END;', (err, result) => {
          done()
          if (err) return callback(err)
          this.query(sql, callback, true)
        })
      } else {
        // call `done()` to release the client back to the pool
        done()
        callback(err, result)
      }
    })
  })
}

function queryError (error) {
  error.msg = error.message
  return error
}

function truncateSql (sql) {
  if (sql.length < 300) return sql
  return sql.slice(0, 299) + '...'
}

Client.prototype.connect = function (callback) {
  this._client.connect(err => {
    if (err) {
      console.log('Could not connect to the database: ' + err.message)
      process.exit(1)
    } else {
      const tables = [
        `CREATE TABLE IF NOT EXISTS "koop:info" (id varchar PRIMARY KEY, info JSONB)`,
        `CREATE TABLE IF NOT EXISTS "koop:services" (id SERIAL PRIMARY KEY, source varchar, url varchar, slug varchar)`
      ]
      _(tables)
      .each(table => {

      })
    }
  })
}

Client.prototype.disconnect = function () {
  this._client.end()
}

module.exports = Client
