var SM = require('sphericalmercator')
var merc = new SM({ size: 256 })
var operators = ['>', '<', '=', '>=', '<=', 'like', 'ilike']

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
  console.log(options)
  var parsed = ''
  if (options.where === '1=1') options.where = null
  if (options.where) {
    parsed = ' WHERE ' + Geoservices.parseWhere(options.where, options.fields)
    // DEPRECATED: idFilter will be removed in 2.0 in favor of using export streams
    if (options.idFilter) parsed += ' AND ' + options.idFilter
  } else if (options.idFilter) {
    parsed = ' WHERE ' + options.idFilter
  }
  parsed += addGeometry(options)
  parsed += addOrder(options)
  var limit = options.limit || options.resultRecordCount
  var offset = options.offset || options.resultOffset
  if (limit) parsed += ' LIMIT ' + limit
  if (offset) parsed += ' OFFSET ' + offset
  return parsed || ''
}

/**
 * Stubs a simplified select statement
 *
 * @param {object} options - includes the table name and layer
 * @return {string} - a sql select statement
 * @private
 */
function stubSimplified (options) {
  return "select id, feature->'properties' as props, st_asgeojson(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->'geometry'), " + options.simplify + ')) as geom from "' + options.table + ':' + (options.layer || 0) + '"'
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
    fragment = options.where || options.idFilter ? ' AND ' : ' WHERE '
    var bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
    return fragment + "ST_GeomFromGeoJSON(feature->>'geometry') && ST_SetSRID('BOX3D(" + bbox + ")'::box3d,4326)"
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
      if (geometry.split(',').length === 4) {
        geom = bbox
        var extent = geometry.split(',')
        geom.xmin = extent[0]
        geom.ymin = extent[1]
        geom.xmax = extent[2]
        geom.ymax = extent[3]
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
  var tokens = tokenize(where)
  if (fields) {
    tokens = decodeDomains(tokens, fields)
  }
  return translate(tokens).replace(/\slike\s/ig, ' ILIKE ')
}

/**
 * Take arbitrary sql and turns it into a consistent set of tokens
 */
function tokenize (sql) {
  // normalize all the binary expressions
  sql = pad(sql)
  var temp
  // find any multi-word tokens and replace the spaces with a special character
  var regex = /'\S+\s\S+'/g
  while ((temp = regex.exec(sql)) !== null) {
    var field = temp[0].replace(/\s/, '|@')
    sql = sql.replace(temp[0], field)
  }
  return sql.split(' ')
}

/**
 * Normalize binary operations to consistent spacing
 */
function pad (sql) {
  var operators = [
    {regex: />=/, string: '>='},
    {regex: /<=/, string: '<='},
    {regex: /[^><]=/, string: '='},
    {regex: />(?!=)/, string: '>'},
    {regex: /<[^=]/, string: '<'}
  ]
  var padded = operators.reduce(function (statement, op) {
    return statement.replace(op.regex, ' ' + op.string + ' ')
  }, sql)
  return padded.replace(/\s\s/g, ' ')
}

/**
 * Iterate through all tokens and replace values that belong to a coded domain
 * @param {array} tokens - a set of tokens for a sql where clause
 * @param {array} fields - the set of fields from a geoservices compatible service
 * @return {array} a set of tokens where any coded values have been decoded
 */
function decodeDomains (tokens, fields) {
  return tokens.map(function (token, i) {
    if (i < 2) return token
    var left = tokens[i - 2]
    var middle = tokens[i - 1]
    var right = token
    // if this set of 3 tokens makes a binary operation then check if we need to apply a domain
    if (isBinaryOp(left, middle, right)) return applyDomain(left, right, fields)
    else return token
  })
}

/**
 * Check whether 3 tokens make up a binary operation
 */
function isBinaryOp (left, middle, right) {
  if (!left || !middle || !right) return false
  return operators.indexOf(middle) > -1
}

/**
 * Check for any coded values in the fields
 * if we find a match, replace value with the coded val
 *
 * @param {string} fieldName - the name of field to look for
 * @param {number} value - the coded value
 * @param {Array} fields - a list of fields to use for coded value replacements
 */
function applyDomain (fieldName, value, fields) {
  var temp = value.replace(/^\(+|\)+$/, '')
  fields.forEach(function (field) {
    if (field.domain && (field.domain.name && field.domain.name === fieldName)) {
      field.domain.codedValues.forEach(function (coded) {
        if (parseInt(coded.code, 10) === parseInt(temp, 10)) {
          value = value.replace(temp, coded.name)
        }
      })
    }
  })
  return value
}

/**
 * Translate tokens to be compatible with postgres json
 */
function translate (tokens) {
  var parts = tokens.map(function (token, i) {
    var middle = tokens[i + 1]
    if (!middle) return token
    // if this is a field name wrap it in postgres json
    var left = jsonify(token, middle)
    var right = removeTrailingParen(tokens[i + 2])
    // if this is a numeric operation cast to float
    return cast(left, middle, right)
  })
  return parts.join(' ').replace(/\|@/g, ' ')
}

/**
 * Removes the trailing parameter from a sql token
 */
function removeTrailingParen (token) {
  if (!token) return undefined
  var paren = token.indexOf(')') > -1
  if (paren) return token.slice(0, paren)
  else return token
}

/**
 * Apply postgres JSON selects where appropriate
 */
function jsonify (token, next) {
  var leading = ''
  var lastPar = token.lastIndexOf('(')
  if (lastPar > -1) {
    leading = token.slice(0, lastPar + 1)
    token = token.replace(/\(/g, '')
  }
  if (operators.indexOf(next) > -1) return leading + "feature->'properties'->>'" + token.replace(/'|"/g, '') + "'"
  else return leading + token
}

/**
 * Cast a JSON selector to float if this is a numeric operation
 */
function cast (left, middle, right) {
  var numericOp = ['>', '<', '=', '>=', '<='].indexOf(middle) > -1 && isNumeric(right)
  if (numericOp) return '(' + left + ')::float'
  else return left
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

function isNumeric (num) {
  return (num >= 0 || num < 0)
}

module.exports = Geoservices
