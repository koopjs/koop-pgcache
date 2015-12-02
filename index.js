var Pg = require('pg')
var SM = require('sphericalmercator')
var merc = new SM({ size: 256 })
var pkg = require('./package')
var Indexing = require('./lib/indexing')
var Table = require('./lib/table')

module.exports = {
  type: 'cache',
  name: 'postgis',
  version: pkg.version,

  geohashPrecision: 8,
  infoTable: 'koopinfo',
  timerTable: 'kooptimers',
  limit: 2000,

  /**
   * Connect to the db with a connection string
   *
   * @param {string} conn - the connection string to the db with user/pass/host/dbname
   * @param {Object} koop - an instance of koop, mainlt for central/shared logging
   * @param {function} optional callback for when the db is ready
   */
  connect: function (conn, koop, callback) {
    var self = this
    // use the koop logger
    this.log = koop.log
    // save the connection string
    this.conn = conn

    this.client = new Pg.Client(conn)
    this.client.connect(function (err) {
      if (err) {
        console.log('Could not connect to the database: ' + err.message)
        process.exit()
      } else {
        // Inject dependencies
        Indexing.query = self.query.bind(self)
        Table.query = self.query.bind(self)

        // creates table only if they dont exist
        Table.create(self.infoTable, '(id varchar(255) PRIMARY KEY, info JSON)', null)
        Table.create(self.timerTable, '(id varchar(255) PRIMARY KEY, expires varchar(25))', null)
      }
      if (callback) {
        callback()
      }
    })
    return this
  },

  addIndexes: Indexing.addIndexes,

  insert: Table.createFeatureTable,

  insertPartial: Table.insertFeatures,

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
        var clause = this.createWhereFromSql(options.where)
        select += ' WHERE ' + clause
      } else {
        select += ' WHERE ' + options.where
      }
    }

    var box = this.parseGeometry(options.geometry)
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
        var clause = this.createWhereFromSql(options.where)
        select += ' WHERE ' + clause
      } else {
        select += ' WHERE ' + options.where
      }
    }

    var box = this.parseGeometry(options.geometry)
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
    this.query('update ' + this.infoTable + " set info = '" + JSON.stringify(info) + "' where id = '" + table + ":info'", function (err, result) {
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
   * Check for any coded values in the fields
   * if we find a match, replace value with the coded val
   *
   * @param {string} fieldName - the name of field to look for
   * @param {number} value - the coded value
   * @param {Array} fields - a list of fields to use for coded value replacements
   */
  applyCodedDomains: function (fieldName, value, fields) {
    fields.forEach(function (field) {
      if (field.domain && (field.domain.name && field.domain.name === fieldName)) {
        field.domain.codedValues.forEach(function (coded) {
          if (parseInt(coded.code, 10) === parseInt(value, 10)) {
            value = coded.name
          }
        })
      }
    })
    return value
  },

  /**
   * Creates a "range" filter for querying numeric values
   *
   * @param {string} sql - a sql where clause
   * @param {Array} fields - a list of fields in to support coded value domains
   */
  createRangeFilterFromSql: function (sql, fields) {
    var terms, type

    if (sql.indexOf(' >= ') > -1) {
      terms = sql.split(' >= ')
      type = '>='
    } else if (sql.indexOf(' <= ') > -1) {
      terms = sql.split(' <= ')
      // paramIndex = 1
      type = '<='
    } else if (sql.indexOf(' = ') > -1) {
      terms = sql.split(' = ')
      // paramIndex = 1
      type = '='
    } else if (sql.indexOf(' > ') > -1) {
      terms = sql.split(' > ')
      // paramIndex = 1
      type = '>'
    } else if (sql.indexOf(' < ') > -1) {
      terms = sql.split(' < ')
      // paramIndex = 1
      type = '<'
    }

    if (terms.length !== 2) { return }

    var fieldName = terms[0].replace(/\'([^\']*)'/g, '$1')
    var value = terms[1]

    // check for fields and apply any coded domains
    if (fields) {
      value = this.applyCodedDomains(fieldName, value, fields)
    }

    var field = " (feature->'properties'->>'" + fieldName + "')"

    if (parseInt(value, 10) || parseInt(value, 10) === 0) {
      if (((parseFloat(value) === parseInt(value, 10)) && !isNaN(value)) || value === 0) {
        field += '::float::int'
      } else {
        field += '::float'
      }
      return field + ' ' + type + ' ' + value
    } else {
      return field + ' ' + type + " '" + value.replace(/'/g, '') + "'"
    }
  },

  /**
  * Create a "like" filter for query string values
  *
  * @param {string} sql - a sql where clause
  * @param {Array} fields - a list of fields in to support coded value domains
  */
  createLikeFilterFromSql: function (sql, fields) {
    var terms = sql.split(' like ')
    if (terms.length !== 2) { return }

    // replace N for unicode values so we can rehydrate filter pages
    var value = terms[1].replace(/^N'/g, "'") // .replace(/^\'%|%\'$/g, '')
    // to support downloads we set quotes on unicode fieldname, here we remove them
    var fieldName = terms[0].replace(/\'([^\']*)'/g, '$1')

    // check for fields and apply any coded domains
    if (fields) {
      value = this.applyCodedDomains(fieldName, value, fields)
    }

    var field = " (feature->'properties'->>'" + fieldName + "')"
    return field + ' ilike ' + value
  },

  /**
   * Determines if a range or like filter is needed   * appends directly to the sql passed in
   *
   * @param {string} sql - a sql where clause
   * @param {Array} fields - a list of fields in to support coded value domains
   */
  createFilterFromSql: function (sql, fields) {
    if (sql.indexOf(' like ') > -1) {
      // like
      return this.createLikeFilterFromSql(sql, fields)
    } else if (sql.indexOf(' < ') > -1 || sql.indexOf(' > ') > -1 || sql.indexOf(' >= ') > -1 || sql.indexOf(' <= ') > -1 || sql.indexOf(' = ') > -1) {
      // part of a range
      return this.createRangeFilterFromSql(sql, fields)
    }
  },

  /**
   * Creates a viable SQL where clause from a passed in SQL (from a url "where" param)
   *
   * @param {string} where - a sql where clause
   * @param {Array} fields - a list of fields in to support coded value domains
   * @returns {string} sql
   */
  createWhereFromSql: function (where, fields) {
    var self = this
    var terms = where.split(' AND ')
    var andWhere = []
    var orWhere = []
    var pairs

    terms.forEach(function (term) {
      // trim spaces
      term = term.trim()
      // remove parens
      term = term.replace(/(^\()|(\)$)/g, '')
      pairs = term.split(' OR ')
      if (pairs.length > 1) {
        pairs.forEach(function (item) {
          orWhere.push(self.createFilterFromSql(item, fields))
        })
      } else {
        pairs.forEach(function (item) {
          andWhere.push(self.createFilterFromSql(item, fields))
        })
      }
    })
    var sql = []
    if (andWhere.length) {
      sql.push(andWhere.join(' AND '))
    } else if (orWhere.length) {
      sql.push('(' + orWhere.join(' OR ') + ')')
    }
    return sql.join(' AND ')
  },

  /**
   * Get features out of the db
   *
   * @param {string} id - the dataset id to insert into
   * @param {Object} options - optional params used for filtering features (where, geometry, etc)
   * @param {function} callback - the callback when the query returns
   */
  select: function (id, options, callback) {
    var self = this
    var layer = (options.layer || 0)

    this.query('select info from "' + this.infoTable + '" where id=\'' + (id + ':' + layer + ':info') + "'", function (err, result) {
      if (err || !result || !result.rows || !result.rows.length) {
        callback(new Error('Resource not found'), [])
      } else if (result.rows[0].info.status === 'processing' && !options.bypassProcessing) {
        callback(null, [{ status: 'processing' }])
      } else {
        var info = result.rows[0].info
        var select
        if (options.simplify) {
          select = "select id, feature->'properties' as props, st_asgeojson(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->'geometry'), " + options.simplify + ')) as geom from "' + id + ':' + (options.layer || 0) + '"'
        } else {
          select = 'select id, feature->\'properties\' as props, feature->\'geometry\' as geom from "' + id + ':' + layer + '"'
        }

        // parse the where clause
        if (options.where) {
          if (options.where !== '1=1') {
            var clause = self.createWhereFromSql(options.where, options.fields)
            select += ' WHERE ' + clause
          } else {
            select += ' WHERE ' + options.where
          }
          if (options.idFilter) {
            select += ' AND ' + options.idFilter
          }
        } else if (options.idFilter) {
          select += ' WHERE ' + options.idFilter
        }

        // parse the geometry param from GeoServices REST
        var box = self.parseGeometry(options.geometry)
        if (box) {
          select += (options.where || options.idFilter) ? ' AND ' : ' WHERE '
          var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
          select += "ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
        }

        // TODO don't do a count here, limits shouldn't be set at the DB level
        self.query(select.replace(/ id, feature->'properties' as props, feature->'geometry' as geom /, ' count(*) as count '), function (err, result) {
          if (!options.limit && !err && result.rows.length && (result.rows[0].count > self.limit && options.enforce_limit)) {
            callback(null, [{
              exceeds_limit: true,
              type: 'FeatureCollection',
              features: [{}],
              name: info.name,
              sha: info.sha,
              info: info.info,
              updated_at: info.updated_at,
              retrieved_at: info.retrieved_at,
              expires_at: info.expires_at,
              count: result.rows[0].count
            }])
          } else {
            if (options.order_by && options.order_by.length) {
              select += ' ' + self._buildSort(options.order_by)
            } else {
              select += ' ORDER BY id'
            }
            if (options.limit) {
              select += ' LIMIT ' + options.limit
            }
            if (options.offset) {
              select += ' OFFSET ' + options.offset
            }
            self.log.debug('Selecting data', select)
            self.query(select, function (err, result) {
              if (err) self.log.error(err)
              var features = []
              if (result && result.rows && result.rows.length) {
                result.rows.forEach(function (row, i) {
                  features.push({
                    'type': 'Feature',
                    'id': row.id,
                    'geometry': row.geom,
                    'properties': row.props
                  })
                })
              }
              callback(null, [{
                type: 'FeatureCollection',
                features: features,
                name: info.name,
                sha: info.sha,
                info: info.info,
                updated_at: info.updated_at,
                retrieved_at: info.retrieved_at,
                expires_at: info.expires_at,
                count: result.rows.length
              }])
            })
          }
        })
      }
    })
  },

  /**
   * Parses a geometry Object
   * TODO this method needs some to support other geometry types. Right now it assumes Envelopes
   *
   * @param {string} geometry - a geometry used for filtering data spatially
   */
  parseGeometry: function (geometry) {
    var bbox = { spatialReference: {wkid: 4326} }
    var geom

    if (!geometry) return false

    if (typeof geometry === 'string') {
      try {
        geom = JSON.parse(geometry)
      } catch (e) {
        try {
          if (geometry.split(',').length === 4) {
            geom = bbox
            var extent = geometry.split(',')
            geom.xmin = extent[0]
            geom.ymin = extent[1]
            geom.xmax = extent[2]
            geom.ymax = extent[3]
          }
        } catch (error) {
          this.log.error('Error building bbox from query ' + geometry)
        }
      }
    } else {
      geom = geometry
    }

    if (geom && (geom.xmin || geom.xmin === 0) && (geom.ymin || geom.ymin === 0) && geom.spatialReference && geom.spatialReference.wkid !== 4326) {
      // is this a valid geometry Object that has a spatial ref different than 4326?
      var mins = merc.inverse([geom.xmin, geom.ymin])
      var maxs = merc.inverse([geom.xmax, geom.ymax])

      bbox.xmin = mins[0]
      bbox.ymin = mins[1]
      bbox.xmax = maxs[0]
      bbox.ymax = maxs[1]
    } else if (geom && geom.spatialReference && geom.spatialReference.wkid === 4326) {
      bbox = geom
    }
    // check to make sure everything is numeric
    if (this.isNumeric(bbox.xmin) && this.isNumeric(bbox.xmax) &&
      this.isNumeric(bbox.ymin) && this.isNumeric(bbox.ymax)) {
      return bbox
    } else {
      return false
    }
  },

  /**
  * Creates a SQL ORDER BY statement
  *
  * @param {array} sorts - an array of {field: order} objects
  * @return {string} a well-formed sql order by statement
  */
  _buildSort: function (sorts) {
    var order = 'ORDER BY '
    sorts.forEach(function (field) {
      var name = Object.keys(field)[0]
      order += "feature->'properties'->>'" + name + "' " + field[name] + ', '
    })
    return order.slice(0, -2)
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

  isNumeric: function (num) {
    return (num >= 0 || num < 0)
  },

  /**
   * Get a geohash aggregation for a set of features in the db
   * this will auto-reduce the precision of the geohashes if the given
   * precision exceeds the given limit.
   *
   * @param {string} table - the table to query
   * @param {number} limit - the max number of geohash to send back
   * @param {string} precision - the precision at which to extract geohashes
   * @param {Object} options - optional params like where and geometry
   * @param {function} callback - the callback when the query returns
   */
  geoHashAgg: function (table, limit, precision, options, callback) {
    var self = this
    options.whereFilter = null
    options.geomFilter = null

    // parse the where clause
    if (options.where) {
      if (options.where !== '1=1') {
        var clause = this.createWhereFromSql(options.where)
        options.whereFilter = ' WHERE ' + clause
      } else {
        options.whereFilter = ' WHERE ' + options.where
      }
      // replace ilike and %% for faster filter queries...
      options.whereFilter = options.whereFilter.replace(/ilike/g, '=').replace(/%/g, '')
    }

    var box = this.parseGeometry(options.geometry)
    // parse the geometry into a bbox
    if (box) {
      var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
      options.geomFilter = " ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
    }

    // recursively get geohash counts until we have a precision
    // that reutrns less than the row limit
    // this will return the precision that will return the number
    // of geohashes less than the limit
    var reducePrecision = function (table, p, options, callback) {
      self.countDistinctGeoHash(table, p, options, function (err, count) {
        if (parseInt(count, 0) > limit) {
          reducePrecision(table, p - 1, options, callback)
        } else {
          callback(err, p)
        }
      })
    }

    var agg = {}

    reducePrecision(table, precision, options, function (err, newPrecision) {
      if (err) self.log.error(err)

      var geoHashSelect

      if (newPrecision <= precision) {
        geoHashSelect = 'substring(geohash,0,' + (newPrecision) + ')'
      } else {
        geoHashSelect = 'geohash'
      }

      var sql = 'SELECT count(id) as count, ' + geoHashSelect + ' as geohash from "' + table + '"'

      // apply any filters to the sql
      if (options.whereFilter) {
        sql += options.whereFilter
      }
      if (options.geomFilter) {
        sql += ((options.whereFilter) ? ' AND ' : ' WHERE ') + options.geomFilter
      }

      sql += ' GROUP BY ' + geoHashSelect
      self.log.info('GEOHASH Query', sql)
      self.query(sql, function (err, res) {
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
  },

  /**
   * Get the count of distinct geohashes for a query
   *
   * @param {string} table - the table to query
   * @param {string} precision - the precision at which to extract the distinct geohash counts    * @param {Object} options - optional params like where and geometry
   * @param {function} callback - the callback when the query returns
   */
  countDistinctGeoHash: function (table, precision, options, callback) {
    var countSql = 'select count(DISTINCT(substring(geohash,0,' + precision + '))) as count from "' + table + '"'

    // apply any filters to the sql
    if (options.whereFilter) {
      countSql += options.whereFilter
    }

    if (options.geomFilter) {
      countSql += ((options.whereFilter) ? ' AND ' : ' WHERE ') + options.geomFilter
    }

    this.log.debug(countSql)
    this.query(countSql, function (err, res) {
      if (err) return callback(err, null)
      callback(null, res.rows[0].count)
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
        var clause = this.createWhereFromSql(options.where)
        sql += ' WHERE ' + clause
      } else {
        sql += ' WHERE ' + options.where
      }
      // replace ilike and %% for faster filter queries...
      options.whereFilter = options.whereFilter.replace(/ilike/g, '=').replace(/%/g, '')
    }

    var box = this.parseGeometry(options.geometry)
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
      self.log.error('Error querying', truncateSql(sql), JSON.stringify(err))
    }

    function truncateSql (sql) {
      if (sql.length < 100) return sql
      return sql.slice(0, 99) + '...'
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
