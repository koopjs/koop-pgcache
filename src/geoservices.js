const SM = require('sphericalmercator')
const merc = new SM({ size: 256 })
const operators = ['>', '<', '=', '>=', '<=', 'like', 'ilike']

const Geoservices = {}

/**
 * Create a sql select string from a geoservices query
 *
 * @param {object} - options pertaining to the geoservices query
 * @return {string} sql select statement
 */
Geoservices.makeSelect = function (options) {
  let stub

  if (options.simplify) {
    stub = stubSimplified(options)
  } else if (options.statistics) {
    stub = stubStatistics(options)
  } else {
    stub = stubStandard(options)
  }
  return stub + Geoservices.parse(options)
}

/**
 * Parse a set of geoservice query options into a sql clause
 *
 * @param {object} - options pertaining to the geoservices query
 * @return {string} - a sql clause
 */
Geoservices.parse = function (options) {
  let parsed = ''
  if (options.where === '1=1') options.where = null
  if (options.where) parsed = ' WHERE ' + Geoservices.parseWhere(options.where, options.fields)
  parsed += addGeometry(options)
  parsed += addGroupBy(options)
  parsed += addOrder(options)
  if (options.limit) parsed += ' LIMIT ' + options.limit
  if (options.offset) parsed += ' OFFSET ' + options.offset
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
  return "select id, feature->'properties' as props, ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->'geometry'), " + options.simplify + ')) as geom from "' + options.table + '"'
}

/**
 * Stubs a normal  select statement
 *
 * @param {object} options - includes the table name and layer
 * @return {string} - a sql select statement
 * @private
 */
function stubStandard (options) {
  return `select id, feature->'properties' as props, feature->'geometry' as geom from ${options.table}" `
}

/**
 * Stubs a select statement with statistics
 *
 * @param {object} options - includes the table name and layer
 * @return {string} - a sql select statement
 * @private
 */
function stubStatistics (options) {

}

/**
 * Creates a geometry fragment that can be appended to a sql query
 *
 * @param {object} options - includes whether there is a where statement and a bbox
 * @return {string} - a sql geometry query fragement
 * @private
 */
function addGeometry (options) {
  const box = Geoservices.parseGeometry(options.geometry)
  if (!box) return ''
  if (box) {
    const fragment = options.where || options.idFilter ? ' AND ' : ' WHERE '
    const bbox = box.xmin + ' ' + box.ymin + ',' + box.xmax + ' ' + box.ymax
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
  return (options.order_by && options.order_by.length) ? ' ' + Geoservices.buildSort(options.order_by) : ''
}

/**
 * Adds group by clauses to a sql statement
 */

function addGroupBy (options) {
  if (!options.groupBy) return ''
  const groupBy = {
    fields: null,
    groups: null
  }

  if (Array.isArray(options.groupby)) {
    const fields = []
    const groups = []
    options.groupby.forEach(field => {
      let groupField = `feature->'properties'->>'${field}'`
      groups.push(groupField)
      fields.push(`'${groupField}' as '${field}'`)
    })
    return {
      groups: groups.join(', '),
      fields: fields.join(', ')
    }
  } else {
    return {
      groups: "feature->'properties'->>'" + options.groupby + "'",
      fields: `'${groupBy}' as '${options.groupby}'`
    }
  }
}

/**
 * Parses a geometry Object
 * TODO this method needs some to support other geometry types. Right now it assumes Envelopes
 *
 * @param {string} geometry - a geometry used for filtering data spatially
 * @return {object} returns a bounding box
 */
Geoservices.parseGeometry = function (geometry) {
  let bbox = { spatialReference: {wkid: 4326} }
  let geom

  if (!geometry) return false

  if (typeof geometry === 'string') {
    try {
      geom = JSON.parse(geometry)
    } catch (e) {
      if (geometry.split(',').length === 4) {
        geom = bbox
        const extent = geometry.split(',')
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
    const mins = merc.inverse([geom.xmin, geom.ymin])
    const maxs = merc.inverse([geom.xmax, geom.ymax])

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
  let tokens = tokenize(where)
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
  let temp
  // find any multi-word tokens and replace the spaces with a special character
  const regex = /'\S+\s\S+'/g
  while ((temp = regex.exec(sql)) !== null) {
    const field = temp[0].replace(/\s/, '|@')
    sql = sql.replace(temp[0], field)
  }
  return sql.split(' ')
}

/**
 * Normalize binary operations to consistent spacing
 */
function pad (sql) {
  const operators = [
    {regex: />=/, string: '>='},
    {regex: /<=/, string: '<='},
    {regex: /[^><]=/, string: '='},
    {regex: />(?!=)/, string: '>'},
    {regex: /<[^=]/, string: '<'}
  ]
  const padded = operators.reduce(function (statement, op) {
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
    const left = tokens[i - 2]
    const middle = tokens[i - 1]
    const right = token
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
  const temp = value.replace(/^\(+|\)+$/, '')
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
  const parts = tokens.map(function (token, i) {
    const middle = tokens[i + 1]
    if (!middle) return token
    // if this is a field name wrap it in postgres json
    const left = jsonify(token, middle)
    const right = removeTrailingParen(tokens[i + 2])
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
  const paren = token.indexOf(')') > -1
  if (paren) return token.slice(0, paren)
  else return token
}

/**
 * Apply postgres JSON selects where appropriate
 */
function jsonify (token, next) {
  let leading = ''
  const lastPar = token.lastIndexOf('(')
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
  const numericOp = ['>', '<', '=', '>=', '<='].indexOf(middle) > -1 && isNumeric(right)
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
  let order = 'ORDER BY '
  sorts.forEach(function (field) {
    const name = Object.keys(field)[0]
    order += "feature->'properties'->>'" + name + "' " + field[name] + ', '
  })
  return order.slice(0, -2)
}

function isNumeric (num) {
  return (num >= 0 || num < 0)
}

module.exports = Geoservices
