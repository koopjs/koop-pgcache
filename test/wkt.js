describe('working with spatial references', function () {
  before(function (done) {
    sinon.stub(pgCache, 'query', function (sql, callback) {
      callback(null, sql)
    })
    done()
  })

  after(function (done) {
    pgCache.query.restore()
    done()
  })

  it('should use the proper SQL to get the WKT', function (done) {
    sinon.stub(pgCache, '_extractWKT', function (sql) {
      return sql
    })

    pgCache.getWKT(1, function (err, sql) {
      should.not.exist(err)
      sql.should.equal('SELECT srtext FROM spatial_ref_sys WHERE srid=1;')
      pgCache._extractWKT.restore()
      done()
    })
  })

  it('should use the proper SQL to insert a WKT', function (done) {
    pgCache.insertWKT(1, 'PROJCS["NAD83(HARN) / Washington South (ftUS)",GEOGCS["NAD83(HARN)",DATUM["NAD83_High_Accuracy_Regional_Network",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6152"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4152"]],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG","9003"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",47.33333333333334],PARAMETER["standard_parallel_2",45.83333333333334],PARAMETER["latitude_of_origin",45.33333333333334],PARAMETER["central_meridian",-120.5],PARAMETER["false_easting",1640416.667],PARAMETER["false_northing",0],AUTHORITY["EPSG","2927"],AXIS["X",EAST],AXIS["Y",NORTH]])', function (err, sql) {
      should.not.exist(err)
      sql.should.equal('INSERT INTO spatial_ref_sys (srid, srtext) VALUES (1,\'PROJCS["NAD83(HARN) / Washington South (ftUS)",GEOGCS["NAD83(HARN)",DATUM["NAD83_High_Accuracy_Regional_Network",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6152"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4152"]],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG","9003"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",47.33333333333334],PARAMETER["standard_parallel_2",45.83333333333334],PARAMETER["latitude_of_origin",45.33333333333334],PARAMETER["central_meridian",-120.5],PARAMETER["false_easting",1640416.667],PARAMETER["false_northing",0],AUTHORITY["EPSG","2927"],AXIS["X",EAST],AXIS["Y",NORTH]])\');')
      done()
    })
  })
})
}
