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



  var table = 'test:german:data5'
  var data = require('./fixtures/germany.json')
  var options = {
    name: 'german-data',
    geomType: 'Point',
    features: data.features
  }

  var field = 'ID'
  var outName = 'stat'

  it('should generate a min value', function (done) {
    var type = 'min'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res[0][outName].should.equal(2914)
      done()
    })
  })

  it('should generate a max value', function (done) {
    var type = 'max'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res[0][outName].should.equal(3606)
      done()
    })
  })

  it('should generate a avg value', function (done) {
    var type = 'avg'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      Math.floor(res[0][outName]).should.equal(3427)
      done()
    })
  })

  it('should generate a var value', function (done) {
    var type = 'var'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      Math.floor(res[0][outName]).should.equal(16793)
      done()
    })
  })

  it('should generate a stddev value', function (done) {
    var type = 'stddev'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      Math.floor(res[0][outName]).should.equal(129)
      done()
    })
  })

  it('should generate a count value', function (done) {
    var type = 'count'
    var options = {}

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res[0][outName].should.equal(249)
      done()
    })
  })

  it('should generate grouped count values with a groupby option', function (done) {
    var type = 'count'
    var options = { groupby: 'Land' }

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res.length.should.equal(6)
      done()
    })
  })

  it('should generate grouped count values with multiple groupby options', function (done) {
    var type = 'count'
    var options = { groupby: ['Land', 'Art'] }

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res.length.should.equal(23)
      done()
    })
  })

  it('should generate stats with a geometry filters', function (done) {
    var type = 'count'
    var options = {
      geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532'
    }

    pgCache.getStat(table + ':0', field, outName, type, options, function (err, res) {
      should.not.exist(err)
      res[0][outName].should.equal(26)
      done()
    })
  })

describe('when creating geohash aggregations', function () {
  var gKey = 'test:german:data4'
  var data = require('./fixtures/germany.json')
  var limit = 1000
  var precision = 8
  var options = { name: 'german-data', geomType: 'Point', features: data.features }

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
        pgCache.geoHashAgg(gKey + ':0', limit, precision, {where: 'ID >= 2894 AND ID <= 3401 AND (Land = \'Germany\' OR Land = \'Poland\') AND Art = \'BRL\''}, function (err, res) {
          should.not.exist(err)
          Object.keys(res).length.should.equal(5)
          done()
        })
      })
    })
  })

  it('should return a geohash when passing an OR where clause', function (done) {
    pgCache.remove(gKey + ':0', function (err, result) {
      if (err) throw err
      pgCache.insert(gKey, options, 0, function (e, s) {
        pgCache.geoHashAgg(gKey + ':0', limit, precision, {where: 'ID >= 2894 AND ID <= 3401 OR (Land = \'Germany\' OR Land = \'Poland\') AND Art = \'BRL\''}, function (err, res) {
          should.not.exist(err)
          Object.keys(res).length.should.equal(64)
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
