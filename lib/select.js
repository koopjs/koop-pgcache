var Geoservices = require('./geoservices')

var Select = {
  query: null,
  limit: 2000
}

/**
 * Get features out of the db
 *
 * @param {string} id - the dataset id to insert into
 * @param {Object} options - optional params used for filtering features (where, geometry, etc)
 * @param {function} callback - the callback when the query returns
 */
Select.features = function (id, options, callback) {
  var self = this
  var layer = (options.layer || 0)
  // TODO move this to index
  options.table = id
  this.query('select info from "' + this.infoTable + '" where id=\'' + (id + ':' + layer + ':info') + "'", function (err, result) {
    if (err || !result || !result.rows || !result.rows.length) {
      callback(new Error('Resource not found'), [])
    } else if (result.rows[0].info.status === 'processing' && !options.bypassProcessing) {
      callback(null, [{ status: 'processing' }])
    } else {
      var info = result.rows[0].info
      var select = Geoservices.parse(options)
      self.query(select, function (err, result) {
        if (err) return callback(err)
        var features = processRows(result, options)
        callback(null, createFeatureCollection(features, info))
      })
    }
  })
}

/**
 * Handle the results from returned from a query for features
 *
 * @param {object} result - the results from postgres
 * @param {object} options - includes whether to enforce a row limit
 * @return {array} a set of geojson features parsed from the postgres results
 */
function processRows (result, options) {
  if (!result || !result.rows || !result.rows.length) return []
  // DEPRECATED support for enforce_limit will be removed in the next major version
  if (options.enforce_limit && result.rows.length > Select.limit) return []
  return result.rows.map(transform)
}

/**
 * creates a feature collection with additional info
 *
 * @param {array} features  - a set of geojson features
 * @param {object} info - additional info to add to the geojson
 */
function createFeatureCollection (features, info) {
  return [{
    type: 'FeatureCollection',
    features: features,
    name: info.name,
    sha: info.sha,
    info: info.info,
    updated_at: info.updated_at,
    retrieved_at: info.retrieved_at,
    expires_at: info.expires_at,
    count: features.length
  }]
}

/**
 * Transforms a row from the database into a geojson Feature
 *
 * @param {object} row - a row from the db
 * @return {object} a geojson feature
 */
function transform (row) {
  return {
    'type': 'Feature',
    'id': row.id,
    'geometry': row.geom,
    'properties': row.props
  }
}

module.exports = Select
