var ngeohash = require('ngeohash')
var centroid = require('turf-centroid')

/**
 * Creates a geohash from a features
 * computes the centroid of lines and polygons
 *
 * @param {Object} feature - a geojson feature
 * @param {number} precision - the precision at which the geohash will be created
 * @returns {string} geohash
 */
module.exports = {
  create: function (feature, precision) {
    if (!feature.geometry || !feature.geometry.coordinates) {
      return
    }
    if (feature.geometry.type !== 'Point') {
      feature = centroid(feature)
    }
    var pnt = feature.geometry.coordinates
    return ngeohash.encode(pnt[1], pnt[0], precision)
  }
}
