var Pg = require('pg')
var ngeohash = require('ngeohash')
var centroid = require('turf-centroid')
var SM = require('sphericalmercator')
var merc = new SM({ size: 256 })
var pkg = require('./package')

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
        // creates table only if they dont exist
        self._createTable(self.infoTable, '(id varchar(255) PRIMARY KEY, info JSONB)', null)
        self._createTable(self.timerTable, '(id varchar(255) PRIMARY KEY, expires varchar(25))', null)
      }
      if (callback) {
        callback()
      }
    })
    return this
  },

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

    this._query(select, function (err, result) {
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
    var select = "SELECT ST_AsGeoJSON(ST_Extent(st_geomfromgeojson(feature ->> 'geometry'))) as extent FROM \"" + table + '"'
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

    this._query(select, function (err, result) {
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
    this._query('select info from "' + this.infoTable + '" where id=\'' + table + ":info\'", function (err, result) {
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
    this._query('update ' + this.infoTable + ' set info = \'' + JSON.stringify(info) + '\' where id = \'' + table + ':info\'', function (err, result) {
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

    var field = ' (feature->\'properties\'->>\'' + fieldName + '\')'

    if (parseInt(value, 10) || parseInt(value, 10) === 0) {
      if (((parseFloat(value) === parseInt(value, 10)) && !isNaN(value)) || value === 0) {
        field += '::float::int'
      } else {
        field += '::float'
      }
      return field + ' ' + type + ' ' + value
    } else {
      return field + ' ' + type + ' \'' + value.replace(/'/g, '') + '\''
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
    var value = terms[1].replace(/^N'/g, '\'') // .replace(/^\'%|%\'$/g, '')
    // to support downloads we set quotes on unicode fieldname, here we remove them
    var fieldName = terms[0].replace(/\'([^\']*)'/g, '$1')

    // check for fields and apply any coded domains
    if (fields) {
      value = this.applyCodedDomains(fieldName, value, fields)
    }

    var field = ' (feature->\'properties\'->>\'' + fieldName + '\')'
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

    this._query('select info from "' + this.infoTable + '" where id=\'' + (id + ':' + layer + ':info') + '\'', function (err, result) {
      if (err || !result || !result.rows || !result.rows.length) {
        callback(new Error('Resource not found'), [])
      } else if (result.rows[0].info.status === 'processing' && !options.bypassProcessing) {
        callback(null, [{ status: 'processing' }])
      } else {
        var info = result.rows[0].info
        var select
        if (options.simplify) {
          select = 'select id, feature->\'properties\' as props, st_asgeojson(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->\'geometry\'), ' + options.simplify + ')) as geom from "' + id + ':' + (options.layer || 0) + '"'
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
          select += 'ST_GeomFromGeoJSON(feature->>\'geometry\') && ST_SetSRID(\'BOX3D(' + bbox + ')\'::box3d,4326)'
        }

        // TODO don't do a count here, limits shouldn't be set at the DB level
        self._query(select.replace(/ id, feature->'properties' as props, feature->'geometry' as geom /, ' count(*) as count '), function (err, result) {
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
            self._query(select, function (err, result) {
              if (err) self.log.error(err)
              if (result && result.rows && result.rows.length) {
                var features = []
                  // feature
                result.rows.forEach(function (row, i) {
                  features.push({
                    'type': 'Feature',
                    'id': row.id,
                    'geometry': row.geom,
                    'properties': row.props
                  })
                })
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
              } else {
                callback('Not Found', [{
                  type: 'FeatureCollection',
                  features: []
                }])
              }
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
      order += 'feature->\'properties\'->\'' + name + '\' ' + field[name] + ', '
    })
    return order.slice(0, -2)
  },

  /**
   * Creates a table and inserts features and metadat
   * creates indexes for each property in the features and substring indexes on geohashes
   *
   * @param {string} id - the dataset id to insert into
   * @param {Object} geojson - geojson features
   * @param {number} layerId - the layer id for this dataset
   * @param {function} callback - the callback when the query returns
   */
  insert: function (id, geojson, layerId, callback) {
    var self = this
    var info = {}

    info.name = geojson.name
    info.updated_at = geojson.updated_at
    info.expires_at = geojson.expires_at
    info.retrieved_at = geojson.retrieved_at
    info.status = geojson.status
    info.format = geojson.format
    info.sha = geojson.sha
    info.info = geojson.info
    info.host = geojson.host

    var table = id + ':' + layerId
    var feature = (geojson.length) ? geojson[0].features[0] : geojson.features[0]

    var types = {
      'esriGeometryPolyline': 'Linestring',
      'esriGeometryPoint': 'Point',
      'esriGeometryPolygon': 'Polygon'
    }

    if (!feature) {
      feature = { geometry: { type: geojson.geomType || types[geojson.info.geometryType] } }
    }

    // a list of indexes to create on the new table
    var indexes = [{
      name: 'gix',
      using: 'GIST (ST_GeomfromGeoJSON(feature->>\'geometry\'))'
    }, {
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
      using: 'btree (substring(geohash,0,8))'
    }]

    // for each property in the data create an index
    if (geojson.info && geojson.info.fields) {
      geojson.info.fields.forEach(function (field) {
        var idx = {
          name: field,
          using: 'btree ((feature->\'properties\'->>\'' + field + '\'))'
        }
        indexes.push(idx)
      })
    }

    self._createTable(table, self._buildSchemaFromFeature(feature), indexes, function (err) {
      if (err) {
        callback(err, false)
        return
      }

      // insert each feature
      if (geojson.length) {
        geojson = geojson[0]
      }
      geojson.features.forEach(function (feature) {
        self._query(self._insertFeature(table, feature), function (err) {
          if (err) {
            self.log.error(err)
          }
        })
      })

      // TODO Why not use an update query here?
      self._query('delete from "' + self.infoTable + '" where id=\'' + table + ':info\'', function (err, res) {
        if (err) self.log.error(err)
        self._query('insert into "' + self.infoTable + '" values (\'' + table + ':info\',\'' + JSON.stringify(info).replace(/'/g, '') + '\')', function (err, result) {
          callback(err, true)
        })
      })
    })
  },

  /**
   * Inserts an array of features
   * used as a way to insert pages of features, and only features, not metadata
   *
   * @param {string} id - the dataset id to insert into
   * @param {Object} geojson - geojson features
   * @param {number} layerId - the layer id for this dataset
   * @param {function} callback - the callback when the query returns
   */
  insertPartial: function (id, geojson, layerId, callback) {
    var self = this
    var sql = 'BEGIN;'
    var table = id + ':' + layerId

    geojson.features.forEach(function (feature) {
      sql += self._insertFeature(table, feature)
    })
    sql += 'COMMIT;'
    this._query(sql, function (err, res) {
      if (err) {
        self.log.error('insert partial ERROR %s, %s', err, id)
        self._query('ROLLBACK;', function () {
          callback(err, false)
        })
      } else {
        self.log.debug('insert partial SUCCESS %s', id)
        callback(null, true)
      }
    })
  },

  /**
   * Creates the sql needed to insert the feature
   *
   * @param {string} table - the table to insert into
   * @param {Object} feature - a geojson feature
   * @private
   */
  _insertFeature: function (table, feature) {
    var featurestring = JSON.stringify(feature).replace(/'/g, '')

    if (feature.geometry && feature.geometry.coordinates && feature.geometry.coordinates.length) {
      var geohash = this.createGeohash(feature, this.geohashPrecision)
      return 'insert into "' + table + '" (feature, geohash) VALUES (\'' + featurestring + '\', \'' + geohash + '\');'
    } else {
      return 'insert into "' + table + '" (feature) VALUES (\'' + featurestring + '\');'
    }
  },

  /**
   * Creates a geohash from a features
   * computes the centroid of lines and polygons
   *
   * @param {Object} feature - a geojson feature
   * @param {number} precision - the precision at which the geohash will be created
   * @returns {string} geohash
   */
  createGeohash: function (feature, precision) {
    if (!feature.geometry || !feature.geometry.coordinates) {
      return
    }
    if (feature.geometry.type !== 'Point') {
      feature = centroid(feature)
    }
    var pnt = feature.geometry.coordinates
    return ngeohash.encode(pnt[1], pnt[0], precision)
  },

  /**
   * Removes everything in the DB for a given idea
   * will delete all metadata, timers, and features   *
   * @param {string} id - the dataset id to remove
   * @param {function} callback - the callback when the query returns
   */
  remove: function (id, callback) {
    var self = this
    this._query('select info from "' + this.infoTable + '" where id=\'' + (id + ':info') + '\'', function (err, result) {
      if (err) self.log.error(err)
      if (!result || !result.rows.length) {
        // nothing to remove
        callback(null, true)
      } else {
        self.dropTable(id, function (err, result) {
          if (err) self.log.error(err)
          self._query('delete from "' + self.infoTable + '" where id=\'' + (id + ':info') + '\'', function (err, result) {
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
    this._query('drop table "' + table + '"', callback)
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
    this._createTable(type, '(id varchar(100), host varchar)', null, function (err, result) {
      if (err) {
        callback(err)
      } else {
        self._query('select * from "' + type + '" where id=\'' + info.id + "\'", function (err, res) {
          if (err || !res || !res.rows || !res.rows.length) {
            var sql = 'insert into "' + type + '" (id, host) VALUES (\'' + info.id + '\', \'' + info.host + '\')'
            self._query(sql, function (err, res) {
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
    this._query(sql, function (err, res) {
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
    this._query(sql, function (err, res) {
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
      self._query(sql, function (err, res) {
        callback(err, (res) ? res.rows : null)
      })
    } else {
      sql = 'select * from "' + type + '" where id=\'' + id + "\'"
      self._query(sql, function (err, res) {
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
    this._query('delete from "' + this.timerTable + '" WHERE id=\'' + key + "\'", function (err, res) {
      if (err) {
        callback(err)
      } else {
        self._query('insert into "' + self.timerTable + '" (id, expires) VALUES (\'' + key + '\', \'' + expires_at.getTime() + '\')', function (err, res) {
          callback(err, res)
        })
      }
    })
  },

  /**
   * Gets the current timer for a given table
   * timers are used throttle API calls by preventing providers from   * over-calling an API.
   *
   * @param {string} table - the table to query
   * @param {function} callback - the callback when the query returns
   */
  timerGet: function (table, callback) {
    this._query('select * from "' + this.timerTable + '" where id=\'' + table + '\'', function (err, res) {
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
   * this will auto-reduce the precision of the geohashes if the given   * precision exceeds the given limit.
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
      self._query(sql, function (err, res) {
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
    this._query(countSql, function (err, res) {
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
    var fieldSql = type.toLowerCase() + '(' + fieldName + ')::float as "' + outName + '\"'

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
    this._query(sql, function (err, result) {
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
    self._query(sql, function (err, result) {
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
    this._query(sql, function (err, result) {
      if (err) return callback(err)
      callback(null, result)
    })
  },

  // ---------------
  // PRIVATE METHODS
  // ---------------

  /**
   * Executes SQL again the DB
   * uses connection pooling to connect and query
   *
   * @param {string} sql - the sql to run
   * @param {function} callback - the callback when db returns
   * @private
   */
  _query: function (sql, callback) {
    Pg.connect(this.conn, function (err, client, done) {
      if (err) {
        return console.error('!error fetching client from pool', err)
      }
      client.query(sql, function (err, result) {
        // call `done()` to release the client back to the pool
        done()
        if (callback) {
          callback(err, result)
        }
      })
    })
  },

  /**
   * Creates an index on a given table   *
   * @param {string} table - the table to index
   * @param {string} name - the name of the index    * @param {string} using - the actual field and type of the index
   * @param {function} callback - the callback when the query returns
   * @private
   */
  _createIndex: function (table, name, using, callback) {
    var sql = 'CREATE INDEX ' + name + ' ON "' + table + '" USING ' + using
    this._query(sql, function (err) {
      if (err) {
        callback(err)
      } else if (callback) {
        callback()
      }
    })
  },

  /**
   * Creates a new table
   * checks to see if the table exists, create it if not
   *
   * @param {string} name - the name of the index    * @param {string} schema - the schema to use for the table   * @param {Array} indexes - an array of indexes to place on the table
   * @param {function} callback - the callback when the query returns
   * @private
   */
  _createTable: function (name, schema, indexes, callback) {
    var self = this
    var sql = 'select exists(select * from information_schema.tables where table_name=\'' + name + '\')'
    this._query(sql, function (err, result) {
      if (err) {
        callback('Failed to create table ' + name)
      } else {
        if (result && !result.rows[0].exists) {
          var create = 'CREATE TABLE "' + name + '" ' + schema
          self.log.info(create)
          self._query(create, function (err, result) {
            if (err) {
              callback('Failed to create table ' + name + ' error:' + err)
              return
            }

            if (indexes && indexes.length) {
              var indexName = name.replace(/:|-/g, '')
              var next = function (idx) {
                if (!idx) {
                  if (callback) {
                    callback()
                  }
                } else {
                  self._createIndex(name, indexName + '_' + idx.name, idx.using, function () {
                    next(indexes.pop())
                  })
                }
              }
              next(indexes.pop())
            } else {
              if (callback) {
                callback()
              }
            }
          })
        } else if (callback) {
          callback()
        }
      }
    })
  },

  /**
   * Builds a table schema from a geojson feature
   * each schema in the db is essentially the same except for geometry type   * which is based off the geometry of the feature passed in here
   *
   * @param {Object} feature - a geojson feature   * @returns {string} schema
   * @private
   */
  _buildSchemaFromFeature: function (feature) {
    var schema = '('
    var type
    if (feature && feature.geometry && feature.geometry.type) {
      type = feature.geometry.type.toUpperCase()
    } else {
      // default to point geoms
      type = 'POINT'
    }
    var props = ['id SERIAL PRIMARY KEY', 'feature JSONB', 'geom Geometry(' + type + ', 4326)', 'geohash varchar(10)']
    schema += props.join(',') + ')'
    return schema
  }
}
