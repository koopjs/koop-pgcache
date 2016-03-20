var Pg = require('pg')
var pkg = require('./package')
var Indexes = require('./lib/indexes')
var Table = require('./lib/table')
var Geoservices = require('./lib/geoservices')
var Geohash = require('./lib/geohash')
var Select = require('./lib/select')
var ExportStream = require('./lib/exportStream')

module.exports = {
  type: 'cache',
  plugin_name: 'postgis',
  version: pkg.version,
  infoTable: 'koopinfo',
  timerTable: 'kooptimers',

  /**
   * Connect to the db with a connection string
   *
   * @param {string} conn - the connection string to the db with user/pass/host/dbname
   * @param {Object} koop - an instance of koop, mainlt for central/shared logging
   * @param {function} optional callback for when the db is ready
   */
  connect: function (conn, koop, callback, retried) {
    var self = this
    // use the koop logger
    this.log = koop.log
    // save the connection string
    this.conn = conn

    this.client = new Pg.Client(conn)
    this.client.connect(function (err) {
      if (err) {
        self.log.error('Could not connect to the database: ' + err.message)
        retried = retried || 0
        if (retried < 3) {
          setTimeout(function () {
            self.connect(conn, koop, callback, retried + 1)
          }, retried * 3000)
        } else {
          process.exit(1)
        }
      } else {
        // Inject dependencies
        [Indexes, Table, Geohash, Select].forEach(function (lib) { lib.query = self.query.bind(self) })
        // creates table only if they dont exist
        Table.create(self.infoTable, '(id varchar(255) PRIMARY KEY, info JSON)', null)
        Table.create(self.timerTable, '(id varchar(255) PRIMARY KEY, expires varchar(25))', null)
      }
      if (callback) callback()
    })
    return this
  },

  select: Select.features,

  addIndexes: Indexes.add,

  insert: Table.createFeatureTable,

  insertPartial: Table.insertFeatures,

  geoHashAgg: Geohash.aggregate,

  createExportStream: function (table, options) { return ExportStream.create(this.conn, table, options) },

  /**
   * Gets the count of all features in a table
   *
   * @param {string} table - the table name
   * @param {Object} options - optional params from the query string: where, geometry, order_by
   * @param {function} callback - returns the count
   */
  getCount: function (table, options, callback) {
    var self = this
    var select = 'select count(*) as count from "' + table + '"'
    if (options.where) {
      if (options.where !== '1=1') {
        var clause = Geoservices.parseWhere(options.where)
        select += ' WHERE ' + clause
      } else {
        select += ' WHERE ' + options.where
      }
    }

    var box = Geoservices.parseGeometry(options.geometry)
    if (box) {
      select += (options.where) ? ' AND ' : ' WHERE '
      var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
      select += "ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
    }

    this.query(select, function (err, result) {
      if (err || !result || !result.rows || !result.rows.length) {
        var error = new Error('Resource not found')
        error.table = table
        callback(error, null)
      } else {
        self.log.debug('Get Count', result.rows[0].count, select)
        callback(null, parseInt(result.rows[0].count, 10))
      }
    })
  },

  /**
   * Gets the extent of all features in a table
   *
   * @param {string} table - the table name
   * @param {Object} options - optional params from the querystring like where and geometry
   * @param {function} callback - returns the count
   */
  getExtent: function (table, options, callback) {
    var self = this
    var select = 'SELECT ST_AsGeoJSON(ST_Extent(st_geomfromgeojson(feature ->> \'geometry\'))) as extent FROM "' + table + '"'
    if (options.where) {
      if (options.where !== '1=1') {
        var clause = Geoservices.parseWhere(options.where)
        select += ' WHERE ' + clause
      } else {
        select += ' WHERE ' + options.where
      }
    }

    var box = Geoservices.parseGeometry(options.geometry)
    if (box) {
      select += (options.where) ? ' AND ' : ' WHERE '
      var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
      select += "ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
    }

    this.query(select, function (err, result) {
      if (err || !result || !result.rows || !result.rows.length) {
        var error = new Error('Resource not found')
        error.table = table
        callback(error, null)
      } else {
        var bbox = JSON.parse(result.rows[0].extent).coordinates
        var extent = {
          xmin: bbox[0][0][0],
          ymin: bbox[0][0][1],
          xmax: bbox[0][2][0],
          ymax: bbox[0][2][1],
          spatialReference: {
            wkid: 4326,
            latestWkid: 4326
          }
        }
        self.log.debug('Get Extent %s %s', table, extent)
        callback(null, extent)
      }
    })
  },

  /**
   * Gets the info/metadata from the koopinfo table in the db
   *
   * @param {string} table - the table name
   * @param {function} callback - returns the info Object
   */
  getInfo: function (table, callback) {
    this.query('select info from "' + this.infoTable + '" where id=\'' + table + ":info'", function (err, result) {
      if (err || !result || !result.rows || !result.rows.length) {
        var error = new Error('Resource not found')
        error.table = table
        callback(error, null)
      } else {
        var info = result.rows[0].info
        callback(null, info)
      }
    })
  },

  /**
   * Updates/overwrites the info/metadata for dataset in the db
   *
   * @param {string} table - the table name
   * @param {Object} info - the metadata Object to insert into the koopinfo table
   * @param {function} callback - returns the info Object
   */
  updateInfo: function (table, info, callback) {
    this.log.debug('Updating info %s %s', table, info.status)
    info = JSON.stringify(info).replace(/'/g, "''")
    var sql = 'update ' + this.infoTable + " set info = '" + info + "' where id = '" + table + ":info'"
    this.query(sql, function (err, result) {
      if (err || !result) {
        var error = new Error('Resource not found')
        error.table = table
        callback(error, null)
      } else {
        callback(null, true)
      }
    })
  },

  /**
   * Removes everything in the DB for a given idea
   * will delete all metadata, timers, and features
   * @param {string} id - the dataset id to remove
   * @param {function} callback - the callback when the query returns
   */
  remove: function (id, callback) {
    var self = this
    this.query('select info from "' + this.infoTable + '" where id=\'' + (id + ':info') + "'", function (err, result) {
      if (err) self.log.error(err)
      if (!result || !result.rows.length) {
        // nothing to remove
        callback(null, true)
      } else {
        self.dropTable(id, function (err, result) {
          if (err) self.log.error(err)
          self.query('delete from "' + self.infoTable + '" where id=\'' + (id + ':info') + "'", function (err, result) {
            if (callback) callback(err, true)
          })
        })
      }
    })
  },

  /**
   * Drops a table from the DB
   *
   * @param {string} table - the table to drop
   * @param {function} callback - the callback when the query returns
   */
  dropTable: function (table, callback) {
    this.query('drop table "' + table + '"', callback)
  },

  /**
   * Register a new service in the DB with the given type and info
   *
   * @param {string} type - the type of service: agol, socrata, ckan, etc.
   * @param {Object} info - Object containing a host and id for this service
   * @param {function} callback - the callback when the query returns
   */
  serviceRegister: function (type, info, callback) {
    var self = this
    Table.create(type, '(id varchar(100), host varchar)', null, function (err, result) {
      if (err) {
        callback(err)
      } else {
        self.query('select * from "' + type + '" where id=\'' + info.id + "'", function (err, res) {
          if (err || !res || !res.rows || !res.rows.length) {
            var sql = 'insert into "' + type + '" (id, host) VALUES (\'' + info.id + "', '" + info.host + "')"
            self.query(sql, function (err, res) {
              callback(err, true)
            })
          } else {
            callback(err, true)
          }
        })
      }
    })
  },

  /**
   * Gets the count of the number of services registered for a given type
   *
   * @param {string} type - the type of service: agol, socrata, ckan, etc.
   * @param {function} callback - the callback when the query returns
   */
  serviceCount: function (type, callback) {
    var sql = 'select count(*) as count from "' + type + '"'
    this.query(sql, function (err, res) {
      if (err || !res || !res.rows || !res.rows.length) {
        callback(err, 0)
      } else {
        callback(err, res.rows[0].count)
      }
    })
  },

  /**
   * Removes a service for a given type and id from the DB
   *
   * @param {string} type - the type of service: agol, socrata, ckan, etc.
   * @param {string} id - the id to use for the service
   * @param {function} callback - the callback when the query returns
   */
  serviceRemove: function (type, id, callback) {
    var sql = 'delete from "' + type + '" where id=\'' + id + "'"
    this.query(sql, function (err, res) {
      callback(err, true)
    })
  },

  /**
   * Gets a service for a given type and id
   * if no id is sent it returns an array of every service for that type
   *
   * @param {string} type - the type of service: agol, socrata, ckan, etc.
   * @param {string} id - the id to use for the service
   * @param {function} callback - the callback when the query returns
   */
  serviceGet: function (type, id, callback) {
    var self = this
    var sql
    if (!id) {
      sql = 'select * from "' + type + '"'
      self.query(sql, function (err, res) {
        callback(err, (res) ? res.rows : null)
      })
    } else {
      sql = 'select * from "' + type + '" where id=\'' + id + "'"
      self.query(sql, function (err, res) {
        if (err || !res || !res.rows || !res.rows.length) {
          var error = new Error('Resource not found')
          error.id = id
          error.type = type
          callback(error, null)
        } else {
          callback(null, res.rows[0])
        }
      })
    }
  },

  /**
   * Sets new timer for a given table
   *
   * @param {string} table - the table to query
   * @param {function} callback - the callback when the query returns
   */
  timerSet: function (key, expires, callback) {
    var self = this
    var now = new Date()
    var expires_at = new Date(now.getTime() + expires)
    this.query('delete from "' + this.timerTable + '" WHERE id=\'' + key + "'", function (err, res) {
      if (err) {
        callback(err)
      } else {
        self.query('insert into "' + self.timerTable + '" (id, expires) VALUES (\'' + key + "', '" + expires_at.getTime() + "')", function (err, res) {
          callback(err, res)
        })
      }
    })
  },

  /**
   * Gets the current timer for a given table
   * timers are used throttle API calls by preventing providers from
   * over-calling an API.
   *
   * @param {string} table - the table to query
   * @param {function} callback - the callback when the query returns
   */
  timerGet: function (table, callback) {
    this.query('select * from "' + this.timerTable + '" where id=\'' + table + "'", function (err, res) {
      if (err || !res || !res.rows || !res.rows.length) {
        callback(err, null)
      } else {
        if (new Date().getTime() < parseInt(res.rows[0].expires, 10)) {
          callback(err, res.rows[0])
        } else {
          callback(err, null)
        }
      }
    })
  },

  /**
   * Gets a statistic on one field at a time
   * Supports where and geometry filters and group by
   * @param {string} table to get data from
   * @param {string} field to generate stats from
   * @param {string} outName the name of the stat field
   * @param {string} type - the stat type: min, max, avg, count, var, stddev
   * @param {Object} options - optional params for the query: where, geometry, groupBy
   * @param {function} callback - when the query is done
   */
  getStat: function (table, field, outName, type, options, callback) {
    // force var to be variance in SQL
    if (type === 'var') {
      type = 'variance'
    }
    // build sql
    var fieldName
    if (type === 'avg' || type === 'sum' || type === 'variance' || type === 'stddev') {
      fieldName = "(feature->'properties'->>'" + field + "')::float"
    } else {
      fieldName = "feature->'properties'->>'" + field + "'"
    }
    var fieldSql = type.toLowerCase() + '(' + fieldName + ')::float as "' + outName + '"'

    // add groupby
    var groupByAs, groupBy
    if (options.groupby) {
      if (Array.isArray(options.groupby)) {
        var gField
        groupByAs = []
        groupBy = []
        options.groupby.forEach(function (f) {
          gField = "feature->'properties'->>'" + f + "'"
          groupBy.push(gField)
          groupByAs.push(gField + ' as "' + f + '"')
        })
        groupBy = groupBy.join(', ')
        groupByAs = groupByAs.join(', ')
      } else {
        groupBy = "feature->'properties'->>'" + options.groupby + "'"
        groupByAs = groupBy + ' as "' + options.groupby + '"'
      }
    }

    var sql = 'select ' + fieldSql + ((groupByAs) ? ', ' + groupByAs : '') + ' from "' + table + '"'

    // apply where and geometry filter
    if (options.where) {
      if (options.where !== '1=1') {
        var clause = Geoservices.parseWhere(options.where)
        sql += ' WHERE ' + clause
      } else {
        sql += ' WHERE ' + options.where
      }
      // replace ilike and %% for faster filter queries...
      options.whereFilter = options.whereFilter.replace(/ilike/g, '=').replace(/%/g, '')
    }

    var box = Geoservices.parseGeometry(options.geometry)
    if (box) {
      sql += (options.where) ? ' AND ' : ' WHERE '
      var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
      sql += "ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
    }

    if (groupBy) {
      sql += 'group by ' + groupBy
    }

    // issue query
    this.query(sql, function (err, result) {
      if (err) {
        return callback(err)
      }
      callback(null, result.rows)
    })
  },

  /**
   * Gets a WKT from the spatial_ref_sys table
   *
   * @param {integer} srid - the Spatial Reference ID
   * @return {string} wkt - the well known text for the spatial reference system
   */
  getWKT: function (srid, callback) {
    var self = this
    var sql = 'SELECT srtext FROM spatial_ref_sys WHERE srid=' + srid + ';'
    self.query(sql, function (err, result) {
      if (err) return callback(err)
      var wkt
      try {
        wkt = self._extractWKT(result)
      } catch (e) {
        callback(e)
      }
      callback(null, wkt)
    })
  },

  /**
   * Wrap the return from getWKT so we can test the sql statement
   *
   * @param {object} result - response from the DB
   * @return {string} - the body of the first row of the results
   */
  _extractWKT: function (result) {
    if (result.rows[0]) return result.rows[0].srtext
    throw new Error('No WKT found')
  },

  /**
   * Inserts a WKT into the spatial_ref_sys table
   *
   * @param {integer} srid - the Spatial Reference ID
   * @param {string} wkt - the well know ext for the spatial referfence system
   */
  insertWKT: function (srid, wkt, callback) {
    var sql = 'INSERT INTO spatial_ref_sys (srid, srtext) VALUES (' + [srid, "'" + wkt + "'"].join(',') + ');'
    this.query(sql, function (err, result) {
      if (err) return callback(err)
      callback(null, result)
    })
  },

  // ---------------
  // PRIVATE METHODS
  // ---------------

  /**
   * Executes SQL against the DB
   * uses connection pooling to connect and query
   *
   * @param {string} sql - the sql to run
   * @param {function} callback - the callback when db returns
   * @param {boolean} retried - whether this query is being retried after an aborted transaction
   * @private
   */
  query: function (sql, callback, retried) {
    var self = this

    Pg.connect(this.conn, function (error, client, done) {
      if (error) {
        self.log.error('error fetching client from pool', error)
        return callback(error)
      }
      self.log.debug(truncateSql(sql))
      client.query(sql, function (err, result) {
        // this error occurs when we have an aborted transaction
        // we'll try to clear that transaction
        if (err) {
          if (err.code === '25P02' && !retried) return handleBrokenTransaction(client, done)
          logQueryError(err)
        }
        // call `done()` to release the client back to the pool
        done()
        if (callback) callback(err, result)
      })
    })

    function logQueryError (err) {
      err.msg = err.message
      self.log.error('Error querying', JSON.stringify(err))
    }

    function truncateSql (sql) {
      if (sql.length < 300) return sql
      return sql.slice(0, 299) + '...'
    }

    function handleBrokenTransaction (client, done) {
      client.query('END;', function (err, result) {
        done()
        if (err && callback) return callback(err)
        // call query recursively but only once
        self.query(sql, callback, true)
      })
    }
  }
}
