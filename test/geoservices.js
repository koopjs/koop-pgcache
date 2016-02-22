/*global describe, it  */
var Geoservices = require('../lib/geoservices')
var should = require('should') // eslint-disable-line

describe('when parsing where clauses', function () {
  it('should return an order only when no parameters are given', function (done) {
    var options = {
      table: 'foo',
      layer: 0
    }
    var where = Geoservices.parse(options)
    where.should.equal(' ORDER BY id')
    done()
  })

  it('should return an order only when where = 1=1 and no other paramters are given', function (done) {
    var options = {
      where: '1=1'
    }
    var where = Geoservices.parse(options)
    where.should.equal(' ORDER BY id')
    done()
  })

  it('should respect resultOffset', function (done) {
    var options = {
      where: '1=1',
      resultOffset: 100,
      offset: undefined
    }
    var where = Geoservices.parse(options)
    where.should.equal(' ORDER BY id OFFSET 100')
    done()
  })

  it('should respect resultRecordCount', function (done) {
    var options = {
      where: '1=1',
      resultRecordCount: 100,
      limit: undefined
    }
    var where = Geoservices.parse(options)
    where.should.equal(' ORDER BY id LIMIT 100')
    done()
  })

  it('should parse a single filter correctly', function (done) {
    var options = {
      where: 'OBJECTID > 0'
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'OBJECTID')::float > 0 ORDER BY id")
    done()
  })

  it('should parse a single filter correctly when there are no spaces between the field and the filter condition', function (done) {
    var options = {
      where: 'OBJECTID>0'
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'OBJECTID')::float > 0 ORDER BY id")
    done()
  })

  it('should parse multiple filters correctly when using AND', function (done) {
    var options = {
      where: 'OBJECTID > 0 AND FOO > 1'
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'OBJECTID')::float > 0 AND (feature->'properties'->>'FOO')::float > 1 ORDER BY id")
    done()
  })

  it('should parse multiple filters correctly when using AND & OR', function (done) {
    var options = {
      where: "(OBJECTID > 0 AND FOO > 1) OR BAR like 'tree'"
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE ((feature->'properties'->>'OBJECTID')::float > 0 AND (feature->'properties'->>'FOO')::float > 1) OR feature->'properties'->>'BAR' ILIKE 'tree' ORDER BY id")
    done()
  })

  it('should respect field names with spaces', function (done) {
    var options = {
      where: "'Total Precip' > 30"
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'Total Precip')::float > 30 ORDER BY id")
    done()
  })

  it('should not cast fields when they are equal to a string', function (done) {
    var options = {
      where: "Foo = 'Bar'"
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE feature->'properties'->>'Foo' = 'Bar' ORDER BY id")
    done()
  })

  it('should parse clauses with many ids', function (done) {
    var options = {
      where: "ID >= 2894 AND ID <= 2997 AND Land like '%germany%' AND Art like '%BRL%'"
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'ID')::float >= 2894 AND (feature->'properties'->>'ID')::float <= 2997 AND feature->'properties'->>'Land' ILIKE '%germany%' AND feature->'properties'->>'Art' ILIKE '%BRL%' ORDER BY id")
    done()
  })

  it('should parse clauses with parentheticals on the inside', function (done) {
    var options = {
      where: "ID >= 2894 AND ID <= 3401 OR (Land = 'Germany' OR Land = 'Poland') AND Art = 'BRL'"
    }
    var where = Geoservices.parse(options)
    where.should.equal(" WHERE (feature->'properties'->>'ID')::float >= 2894 AND (feature->'properties'->>'ID')::float <= 3401 OR (feature->'properties'->>'Land' = 'Germany' OR feature->'properties'->>'Land' = 'Poland') AND feature->'properties'->>'Art' = 'BRL' ORDER BY id")
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
          name: 'Decoded',
          code: 0
        },
        {
          name: 'Name1',
          code: 1
        }
      ]
    }
  }]

  it('should replace decode a CVD', function (done) {
    var options = {
      where: 'NAME = 0',
      fields: fields
    }
    var where = Geoservices.parse(options)
    console.log(where)
    where.indexOf('Decoded').should.be.above(-1)
    done()
  })
})
