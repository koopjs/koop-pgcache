var SM = require('sphericalmercator')
var merc = new SM({ size: 256 })

var Geoservices = {}

/**
 * Create a sql select string from a geoservices query
 *
 * @param {object} - options pertaining to the geoservices query
 * @return {string} sql select statement
 */
Geoservices.makeSelect = function (options) {
  var stub = options.simplify ? stubSimplified(options) : stubStandard(options)
  return stub + Geoservices.parse(options)
}

/**
 * Parse a set of geoservice query options into a sql clause
 *
 * @param {object} - options pertaining to the geoservices query
 * @return {string} - a sql clause
 */
Geoservices.parse = function (options) {
  var parsed
  if (options.where) parsed = ' WHERE ' + Geoservices.parseWhere(options.where)
  parsed += addGeometry(options)
  parsed += addOrder(options)
  if (options.limit) parsed += ' LIMIT ' + options.limit
  if (options.offset) parsed += ' OFFSET ' + options.offset
  return parsed
}

/**
 * Stubs a simplified select statement
 *
 * @param {object} options - includes the table name and layer
 * @return {string} - a sql select statement
 * @private
 */
function stubSimplified (options) {
  return 'select id, feature->\'properties\' as props, st_asgeojson(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->\'geometry\'), ' + options.simplify + ')) as geom from "' + options.table + ':' + (options.layer || 0) + '"'
}

/**
 * Stubs a normal  select statement
 *
 * @param {object} options - includes the table name and layer
 * @return {string} - a sql select statement
 * @private
 */
function stubStandard (options) {
  return 'select id, feature->\'properties\' as props, feature->\'geometry\' as geom from "' + options.table + ':' + options.layer + '"'
}

/**
 * Creates a geometry fragment that can be appended to a sql query
 *
 * @param {object} options - includes whether there is a where statement and a bbox
 * @return {string} - a sql geometry query fragement
 * @private
 */
function addGeometry (options) {
  var fragment
  var box = Geoservices.parseGeometry(options.geometry)
  if (!box) return ''
  if (box) {
    fragment = options.where ? ' AND ' : ' WHERE '
    var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
    return fragment + 'ST_GeomFromGeoJSON(feature->>\'geometry\') && ST_SetSRID(\'BOX3D(' + bbox + ')\'::box3d,4326)'
  }
}

/**
 * Adds an order by clause on to a sql statement
 *
 * @param {object} options - includes a set of order by fields
 * @return {string} - a sql order by fragement
 * @private
 */
function addOrder (options) {
  return (options.order_by && options.order_by.length) ? ' ' + Geoservices.buildSort(options.order_by) : ' ORDER BY id'
}

/**
 * Parses a geometry Object
 * TODO this method needs some to support other geometry types. Right now it assumes Envelopes
 *
 * @param {string} geometry - a geometry used for filtering data spatially
 * @return {object} returns a bounding box
 */
Geoservices.parseGeometry = function (geometry) {
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
  if (isNumeric(bbox.xmin) && isNumeric(bbox.xmax) &&
    isNumeric(bbox.ymin) && isNumeric(bbox.ymax)) {
    return bbox
  } else {
    return false
  }
}

/**
 * Creates a viable SQL where clause from a passed in SQL (from a url "where" param)
 *
 * @param {string} where - a sql where clause
 * @param {Array} fields - a list of fields in to support coded value domains
 * @return {string} sql
 */
Geoservices.parseWhere = function (where, fields) {
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
        orWhere.push(createFilterFromSql(item, fields))
      })
    } else {
      pairs.forEach(function (item) {
        andWhere.push(createFilterFromSql(item, fields))
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
}

/**
* Creates a SQL ORDER BY statement
*
* @param {array} sorts - an array of {field: order} objects
* @return {string} a well-formed sql order by statement
*/
Geoservices.buildSort = function (sorts) {
  var order = 'ORDER BY '
  sorts.forEach(function (field) {
    var name = Object.keys(field)[0]
    order += "feature->'properties'->>'" + name + "' " + field[name] + ', '
  })
  return order.slice(0, -2)
}

/**
 * Check for any coded values in the fields
 * if we find a match, replace value with the coded val
 *
 * @param {string} fieldName - the name of field to look for
 * @param {number} value - the coded value
 * @param {Array} fields - a list of fields to use for coded value replacements
 */
Geoservices.applyCodedDomains = function (fieldName, value, fields) {
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
}

/**
* Create a "like" filter for query string values
*
* @param {string} sql - a sql where clause
* @param {Array} fields - a list of fields in to support coded value domains
*/
function createLikeFilterFromSql (sql, fields) {
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
}

/**
 * Creates a "range" filter for querying numeric values
 *
 * @param {string} sql - a sql where clause
 * @param {Array} fields - a list of fields in to support coded value domains
 */
function createRangeFilterFromSql (sql, fields) {
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
    value = Geoservices.applyCodedDomains(fieldName, value, fields)
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
}

/**
 * Determines if a range or like filter is needed   * appends directly to the sql passed in
 *
 * @param {string} sql - a sql where clause
 * @param {Array} fields - a list of fields in to support coded value domains
 */
function createFilterFromSql (sql, fields) {
  if (sql.indexOf(' like ') > -1) {
    // like
    return createLikeFilterFromSql(sql, fields)
  } else if (sql.indexOf(' < ') > -1 || sql.indexOf(' > ') > -1 || sql.indexOf(' >= ') > -1 || sql.indexOf(' <= ') > -1 || sql.indexOf(' = ') > -1) {
    // part of a range
    return createRangeFilterFromSql(sql, fields)
  }
}

function isNumeric (num) {
  return (num >= 0 || num < 0)
}

module.exports = Geoservices
