/*global before, describe, beforeEach, it, afterEach */
var should = require('should'),
  fs = require('fs'),
  Logger = require('./logger'),
  key,
  repoData,
  snowData,
  pgCache

before(function (done) {
  key = 'test:repo:file'
  repoData = require('./fixtures/data.geojson')
  snowData = require('./fixtures/snow.geojson')
  pgCache = require('../')
  var config = JSON.parse(fs.readFileSync(__dirname + '/config.json'))

  pgCache.connect(config.db.conn, {}, function () {
    done()
  })

  // init the koop log based on config params
  config.logfile = __dirname + '/test.log'
  pgCache.log = new Logger(config)
})

describe('pgCache Model Tests', function () {

  describe('when creating DB tables', function () {
    it('create a table w/o erroring', function (done) {
      var name = 'testtable',
        schema = '(id varchar(100), feature json, geom Geometry(POINT, 4326))',
        indexes = []

      pgCache._createTable(name, schema, indexes, function (err) {
        should.not.exist(err)
        done()
      })
    })
  })

  describe('when caching geojson', function () {

    beforeEach(function (done) {
      pgCache.insert(key, repoData[0], 0, function (err) {
        if (err) {
          console.log('insert failed', err)
        }
        done()
      })
    })

    afterEach(function (done) {
      pgCache.remove(key + ':0', done)
    })

    it('should error when missing key is sent', function (done) {
      pgCache.getInfo(key + '-BS:0', function (err, data) {
        should.exist(err)
        done()
      })
    })

    it('should return info', function (done) {
      pgCache.getInfo(key + ':0', function (err, data) {
        should.not.exist(err)
        done()
      })
    })

    it('should update info', function (done) {
      pgCache.updateInfo(key + ':0', {test: true}, function (err, data) {
        should.not.exist(err)
        pgCache.getInfo(key + ':0', function (err, data) {
          should.not.exist(err)
          data.test.should.equal(true)
          done()
        })
      })
    })

    it('should insert data', function (done) {
      var snowKey = 'test:snow:data'
      pgCache.insert(snowKey, snowData, 0, function (error, success) {
        should.not.exist(error)
        success.should.equal(true)

        pgCache.getInfo(snowKey + ':0', function (err, info) {
          should.not.exist(err)
          info.name.should.equal('snow.geojson')

          pgCache.remove(snowKey + ':0', function (err, result) {
            should.not.exist(err)

            pgCache.getInfo(snowKey + ':0', function (err, info) {
              should.exist(err)
              done()
            })
          })
        })
      })
    })

    it('should insert data to an existing table', function (done) {
      pgCache.insertPartial(key, snowData, 0, function (err, success) {
        should.not.exist(err)
        success.should.equal(true)
        done()
      })
    })

    it('should select data from db', function (done) {
      pgCache.select(key, { layer: 0 }, function (error, success) {
        should.not.exist(error)
        should.exist(success[0].features)
        done()
      })
    })

    it('should select data from db with filter', function (done) {
      pgCache.select(key, { layer: 0, where: '\'total precip\' = \'0.31\'' }, function (error, success) {
        should.not.exist(error)
        should.exist(success[0].features)
        success[0].features.length.should.equal(5)
        done()
      })
    })

    it('should select data from db with an OR filter', function (done) {
      pgCache.select(key, { layer: 0, where: '\'total precip\' = \'0.31\' OR \'total precip\' > \'0.5\'' }, function (error, success) {
        should.not.exist(error)
        should.exist(success[0].features)
        success[0].features.length.should.equal(120)
        done()
      })
    })

    it('should insert data with no features', function (done) {
      var snowKey = 'test:snow:data'
      pgCache.insert(snowKey, {name: 'no-data', geomType: 'Point', features: []}, 0, function (error, success) {
        should.not.exist(error)
        success.should.equal(true)
        pgCache.getInfo(snowKey + ':0', function (err, info) {
          should.not.exist(err)
          info.name.should.equal('no-data')
          pgCache.remove(snowKey + ':0', function (err, result) {
            should.not.exist(err)
            pgCache.getInfo(snowKey + ':0', function (err, info) {
              should.exist(err)
              done()
            })
          })
        })
      })
    })

    it('should query data with AND filter', function (done) {
      var gKey = 'test:german:data'
      var data = require('./fixtures/germany.json')

      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function (error, success) {
          should.not.exist(error)
          success.should.equal(true)
          pgCache.select(gKey, { layer: 0, where: 'ID >= 2894 AND ID <= \'2997\''}, function (err, res) {
            if (err) throw err
            should.not.exist(error)
            res[0].features.length.should.equal(7)
            pgCache.remove(gKey + ':0', function (err, result) {
              should.not.exist(err)
              pgCache.getInfo(gKey + ':0', function (err, info) {
                should.exist(err)
                done()
              })
            })
          })
        })
      })
    })

    it('should query data with many AND filters', function (done) {
      var gKey = 'test:german:data2'
      var data = require('./fixtures/germany.json')

      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) console.log(err)
        pgCache.insert(gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function (error, success) {

          should.not.exist(error)
          success.should.equal(true)

          pgCache.select(gKey, { layer: 0, where: 'ID >= 2894 AND ID <= 2997 AND Land like \'%germany%\' AND Art like \'%BRL%\'' }, function (err, res) {
            if (err) console.log(err)

            should.not.exist(error)
            res[0].features.length.should.equal(2)

            pgCache.remove(gKey + ':0', function (err, result) {
              should.not.exist(err)

              pgCache.getInfo(gKey + ':0', function (err, info) {
                should.exist(err)
                done()
              })
            })
          })
        })
      })
    })

    it('should query data with OR filters', function (done) {
      var gKey = 'test:german:data3'
      var data = require('./fixtures/germany.json')

      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function (error, success) {

          should.not.exist(error)
          success.should.equal(true)

          pgCache.select(gKey, { layer: 0, where: 'ID >= 2894 AND ID <= 3401 AND  (Land = \'Germany\' OR Land = \'Poland\')  AND Art = \'BRL\'' }, function (err, res) {
            if (err) throw err
            should.not.exist(error)
            res[0].features.length.should.equal(7)

            pgCache.remove(gKey + ':0', function (err, result) {
              should.not.exist(err)

              pgCache.getInfo(gKey + ':0', function (err, info) {
                should.exist(err)
                done()
              })
            })
          })
        })
      })
    })

    it('should correctly query data with geometry filter', function (done) {
      var gKey = 'test:german:data2'
      var data = require('./fixtures/germany.json')

      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function (error, success) {

          should.not.exist(error)
          success.should.equal(true)

          pgCache.select(gKey, { layer: 0, geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532' }, function (err, res) {
            if (err) throw err
            should.not.exist(error)
            res[0].features.length.should.equal(26)

            pgCache.remove(gKey + ':0', function (err, result) {
              should.not.exist(err)

              pgCache.getInfo(gKey + ':0', function (err, info) {
                should.exist(err)
                done()
              })
            })
          })
        })
      })
    })

    it('should get count', function (done) {
      pgCache.getCount(key + ':0', {}, function (err, count) {
        if (err) throw err
        count.should.equal(417)
        done()
      })
    })

    it('should get feature extent', function (done) {
      pgCache.getExtent(key + ':0', {}, function (err, extent) {
        should.not.exist(err)
        should.exist(extent.xmin)
        should.exist(extent.ymin)
        should.exist(extent.xmax)
        should.exist(extent.ymax)
        done()
      })
    })

  })

  describe('when parsing geometries', function () {
    it('should parse string geometries', function (done) {
      var geom = pgCache.parseGeometry('11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532')
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
      var geom = pgCache.parseGeometry(input)
      geom.xmin.should.equal(-123.75)
      done()
    })

    it('should parse object geometries as strings', function (done) {
      var input = '{"xmin":-15028131.257092925, "ymin":3291933.865166463, "xmax":-10018754.171396945, "ymax":8301310.950862443, "spatialReference":{"wkid":102100, "latestWkid":3857}}'
      var geom = pgCache.parseGeometry(input)
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
      var geom = pgCache.parseGeometry(input)
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

    var value = 0,
      fieldName = 'NAME'

    it('should replace value', function (done) {
      value = pgCache.applyCodedDomains(fieldName, value, fields)
      value.should.equal('Name0')
      done()
    })

  })

  describe('when creating geohash aggregations', function () {
    var gKey = 'test:german:data4',
      data = require('./fixtures/germany.json'),
      limit = 1000,
      precision = 8,
      options = { name: 'german-data', geomType: 'Point', features: data.features }

    it('should create a geohash', function (done) {
      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, options, 0, function (e, s) {
          pgCache.geoHashAgg(gKey + ':0', limit, precision, {}, function (err, res) {
            should.not.exist(err)
            Object.keys(res).length.should.equal(169)
            done()
          })
        })
      })
    })

    it('should return a reduced geohash when passing a low limit', function (done) {
      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, options, 0, function (e, s) {
          pgCache.geoHashAgg(gKey + ':0', 100, precision, {}, function (err, res) {
            should.not.exist(err)
            Object.keys(res).length.should.equal(29)
            done()
          })
        })
      })
    })

    it('should return a geohash when passing where clause', function (done) {
      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, options, 0, function (e, s) {
          pgCache.geoHashAgg(gKey + ':0', limit, precision, {where: 'ID >= 2894 AND ID <= 3401 AND  (Land = \'Germany\' OR Land  = \'Poland\')  AND Art = \'BRL\''}, function (err, res) {
            should.not.exist(err)
            Object.keys(res).length.should.equal(7)
            done()
          })
        })
      })
    })

    it('should return a geohash when passing an OR where clause', function (done) {
      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, options, 0, function (e, s) {
          pgCache.geoHashAgg(gKey + ':0', limit, precision, {where: 'ID >= 2894 AND ID <= 3401 OR (Land = \'Germany\' OR Land  = \'Poland\') AND Art = \'BRL\''}, function (err, res) {
            should.not.exist(err)
            Object.keys(res).length.should.equal(10)
            done()
          })
        })
      })
    })

    it('should return a geohash when passing geometry filter', function (done) {
      pgCache.remove(gKey + ':0', function (err, result) {
        if (err) throw err
        pgCache.insert(gKey, options, 0, function (e, s) {
          pgCache.geoHashAgg(gKey + ':0', limit, precision, {geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532'}, function (err, res) {
            should.not.exist(err)
            Object.keys(res).length.should.equal(17)
            done()
          })
        })
      })
    })
  })

  describe('when getting stats ', function () {
    var table = 'test:german:data5',
      data = require('./fixtures/germany.json'),
      options = {
        name: 'german-data',
        geomType: 'Point',
        features: data.features
      }

    beforeEach(function (done) {
      pgCache.remove(table + ':0', function () {
        pgCache.insert(table, options, 0, function () {
          done()
        })
      })
    })

    afterEach(function (done) {
      pgCache.remove(table + ':0', function () {
        done()
      })
    })

    var field = 'ID',
      outName = 'stat'

    it('should generate a min value', function (done) {
      var type = 'min',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(2914)
        done()
      })
    })

    it('should generate a max value', function (done) {
      var type = 'max',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(3606)
        done()
      })
    })

    it('should generate a avg value', function (done) {
      var type = 'avg',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(3427)
        done()
      })
    })

    it('should generate a var value', function (done) {
      var type = 'var',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(16793)
        done()
      })
    })

    it('should generate a stddev value', function (done) {
      var type = 'stddev',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(130)
        done()
      })
    })

    it('should generate a count value', function (done) {
      var type = 'count',
        options = {}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(249)
        done()
      })
    })

    it('should generate grouped count values with a groupby option', function (done) {
      var type = 'count',
        options = {groupby: 'Land'}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res.length.should.equal(6)
        done()
      })
    })

    it('should generate grouped count values with multiple groupby options', function (done) {
      var type = 'count',
        options = {groupby: ['Land', 'Art']}

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res.length.should.equal(23)
        done()
      })
    })

    it('should generate stats with a geometry filters', function (done) {
      var type = 'count',
        options = {
          geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532'
        }

      pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
        should.not.exist(err)
        res[0][outName].should.equal(26)
        done()
      })
    })

  })

})
