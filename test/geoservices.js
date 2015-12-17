/*global  describe, it  */
var Geoservices = require('../lib/geoservices')
var should = require('should') //eslint-disable-line

describe('when parsing geoservices', function () {
  it('should return an order only when no parameters are given', function (done) {
    var options = {
      table: 'foo',
      layer: 0
    }
    var select = Geoservices.parse(options)
    select.should.equal(' ORDER BY id')
    done()
  })
})

describe('when parsing geometries', function () {
  it('should parse string geometries', function (done) {
    var geom = Geoservices.parseGeometry('11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532')
    geom.xmin.should.equal('11.296916335529545')
    geom.ymin.should.equal('50.976109119993865')
    geom.xmax.should.equal('14.273970437121521')
    geom.ymax.should.equal('52.39566469623532')
    done()
  })

  it('should parse object geometries', function (done) {
    var input = {
      xmin: -123.75,
      ymin: 48.922499263758255,
      xmax: -112.5,
      ymax: 55.7765730186677,
      spatialReference: {
        wkid: 4326
      }
    }
    var geom = Geoservices.parseGeometry(input)
    geom.xmin.should.equal(-123.75)
    done()
  })

  it('should parse object geometries as strings', function (done) {
    var input = '{"xmin":-15028131.257092925, "ymin":3291933.865166463, "xmax":-10018754.171396945, "ymax":8301310.950862443, "spatialReference":{"wkid":102100, "latestWkid":3857}}'
    var geom = Geoservices.parseGeometry(input)
    geom.xmin.should.equal(-135.00000000000892)
    done()
  })

  it('should parse object geometries in 102100', function (done) {
    var input = {
      xmin: -15028131.257092925,
      ymin: 3291933.865166463,
      xmax: -10018754.171396945,
      ymax: 8301310.950862443,
      spatialReference: {
        wkid: 102100
      }
    }
    var geom = Geoservices.parseGeometry(input)
    geom.xmin.should.equal(-135.00000000000892)
    done()
  })
})

describe('when filtering with coded domains', function () {
  var fields = [{
    name: 'NAME',
    type: 'esriFieldTypeSmallInteger',
    alias: 'NAME',
    domain: {
      type: 'codedValue',
      name: 'NAME',
      codedValues: [
        {
          name: 'Name0',
          code: 0
        },
        {
          name: 'Name1',
          code: 1
        }
      ]
    }
  }]

  var value = 0
  var fieldName = 'NAME'

  it('should replace value', function (done) {
    value = Geoservices.applyCodedDomains(fieldName, value, fields)
    value.should.equal('Name0')
    done()
  })
})
