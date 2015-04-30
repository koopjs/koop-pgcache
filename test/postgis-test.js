var should = require('should'),
logger = require('./logger');

before(function (done) {
  key = 'test:repo:file';
  repoData = require('./fixtures/data.geojson');
  snowData = require('./fixtures/snow.geojson');
  pgCache = require('../');
  var config = {
    "db": {
      "conn": "postgres://localhost/koopdev"
    }
  };

  pgCache.connect(config.db.conn, {}, function(){
    done();
  });

  // init the koop log based on config params  
  config.logfile = __dirname + "/test.log";
  pgCache.log = new logger( config );
});

describe('pgCache Model Tests', function(){
      describe('when caching a github file', function(){

        beforeEach(function(done){
          pgCache.insert( key, repoData[0], 0, done);
        });

        afterEach(function(done){
          pgCache.remove( key+':0', done);
        });

        it('should error when missing key is sent', function(done){
          pgCache.getInfo(key+'-BS:0', function( err, data ){
            should.exist( err );
            done();
          });
        });

        it('should return info', function(done){
          pgCache.getInfo(key+':0', function( err, data ){
            should.not.exist( err );
            done();
          });
        });

        it('should update info', function(done){
          pgCache.updateInfo(key+':0', {test: true}, function( err, data ){
            should.not.exist( err );
            pgCache.getInfo(key+':0', function(err, data){
              data.test.should.equal(true);
              done();
            });
          });
        });

        it('should insert, data', function(done){
          var snowKey = 'test:snow:data';
          pgCache.insert( snowKey, snowData, 0, function( error, success ){
            should.not.exist(error);
            success.should.equal( true );
            pgCache.getInfo( snowKey + ':0', function( err, info ){
              should.not.exist(err);
              info.name.should.equal('snow.geojson');
              pgCache.remove(snowKey+':0', function(err, result){
                should.not.exist( err );
                pgCache.getInfo( snowKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });
          });
        });

        it('should select data from db', function(done){
          pgCache.select( key, { layer: 0 }, function( error, success ){
            should.not.exist(error);
            should.exist(success[0].features);
            done();
          });
        });

        it('should select data from db with filter', function(done){
          pgCache.select( key, { layer: 0, where: '\'total precip\' = \'0.31\'' }, function( error, success ){
            should.not.exist(error);
            should.exist(success[0].features);
            success[0].features.length.should.equal(5);
            done();
          });
        });

        it('should insert data with no features', function(done){
          var snowKey = 'test:snow:data';
          pgCache.insert( snowKey, {name: 'no-data', geomType: 'Point', features:[]}, 0, function( error, success ){
            should.not.exist(error);
            success.should.equal( true );
            pgCache.getInfo( snowKey + ':0', function( err, info ){
              should.not.exist(err);
              info.name.should.equal('no-data');
              pgCache.remove(snowKey+':0', function(err, result){
                should.not.exist( err );
                pgCache.getInfo( snowKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });
          });
        });

        it('should query data with AND filter', function(done){
          var gKey = 'test:german:data';
          var data = require('./fixtures/germany.json');

          pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function( error, success ){

            should.not.exist(error);
            success.should.equal( true );

            pgCache.select( gKey, { layer: 0, where: 'ID >= 2894 AND ID <= \'2997\''}, function(err, res){

              should.not.exist(error);
              res[0].features.length.should.equal(7);

              pgCache.remove(gKey+':0', function(err, result){
                should.not.exist( err );

                pgCache.getInfo( gKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });

          });
          });
        });

        it('should query data with many AND filters', function(done){
          var gKey = 'test:german:data2';
          var data = require('./fixtures/germany.json');

          pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function( error, success ){

            should.not.exist(error);
            success.should.equal( true );

            pgCache.select( gKey, { layer: 0, where: 'ID >= 2894 AND ID <= 2997 AND Land like \'%germany%\' AND Art like \'%BRL%\'' }, function(err, res){

              should.not.exist(error);
              res[0].features.length.should.equal(2);

              pgCache.remove(gKey+':0', function(err, result){
                should.not.exist( err );

                pgCache.getInfo( gKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });

          });
          });
        });

        it('should query data with OR filters', function(done){
          var gKey = 'test:german:data3';
          var data = require('./fixtures/germany.json');

          pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function( error, success ){

            should.not.exist(error);
            success.should.equal( true );

            pgCache.select( gKey, { layer: 0, where: 'ID >= 2894 AND ID <= 3401 AND  (Land = \'Germany\' OR Land = \'Poland\')  AND Art = \'BRL\'' },            function(err, res){

              should.not.exist(error);
              res[0].features.length.should.equal(5);

              pgCache.remove(gKey+':0', function(err, result){
                should.not.exist( err );

                pgCache.getInfo( gKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });

          });
          });
        });

        it('should correctly query data with geometry filter', function(done){
          var gKey = 'test:german:data2';
          var data = require('./fixtures/germany.json');

          pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, { name: 'german-data', geomType: 'Point', features: data.features }, 0, function( error, success ){

            should.not.exist(error);
            success.should.equal( true );

            pgCache.select( gKey, { layer: 0, geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532' }, function(err, res){

              should.not.exist(error);
              res[0].features.length.should.equal(26);

              pgCache.remove(gKey+':0', function(err, result){
                should.not.exist( err );

                pgCache.getInfo( gKey + ':0', function( err, info ){
                  should.exist( err );
                  done();
                });
              });
            });

          });
          });
        });

        it('should get count', function(done){
          pgCache.getCount(key+':0', {}, function(err, count){
            count.should.equal(417);
            done();
          });
        });

      });

    describe('when filtering with coded domains', function(){

      var fields = [{
        name: "NAME",
        type: "esriFieldTypeSmallInteger",
        alias: "NAME",
        domain: {
          type: "codedValue",
          name: "NAME",
          codedValues: [
            {
              name: "Name0",
              code: 0
            },
            {
              name: "Name1",
              code: 1
            }
          ]
        }
      }];

      var value = 0,
        fieldName = 'NAME';

      it('should replace value', function(done){
        value = pgCache.applyCodedDomains(fieldName, value, fields);
        value.should.equal('Name0');
        done();
      });

    });

    describe('when creating geohash aggregations', function(){
      var gKey = 'test:german:data4',
        data = require('./fixtures/germany.json'),
        limit = 1000,
        precision = 8,
        options = { name: 'german-data', geomType: 'Point', features: data.features };

      it('should create a geohash', function(done){
        pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, options, 0, function( e, s ){
            pgCache.geoHashAgg(gKey+':0', limit, precision, {}, function(err, res){
              should.not.exist(error);
              Object.keys(res).length.should.equal( 169 );
              done();
            });
          });
        });
      });

      it('should return a reduced geohash when passing a low limit', function(done){
        pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, options, 0, function( e, s ){
            pgCache.geoHashAgg(gKey+':0', 100, precision, {}, function(err, res){
              should.not.exist(error);
              Object.keys(res).length.should.equal( 29 );
              done();
            });
          });
        });
      });

      it('should return a geohash when passing where clause', function(done){
        pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, options, 0, function( e, s ){
            pgCache.geoHashAgg(gKey+':0', limit, precision, {where: 'ID >= 2894 AND ID <= 3401 AND  (Land = \'Germany\' OR Land  = \'Poland\')  AND Art = \'BRL\''}, function(err, res){
              should.not.exist(error);
              Object.keys(res).length.should.equal( 5 );
              done();
            });
          });
        });
      });

      it('should return a geohash when passing geometry filter', function(done){
        pgCache.remove(gKey+':0', function(err, result){
          pgCache.insert( gKey, options, 0, function( e, s ){
            pgCache.geoHashAgg(gKey+':0', limit, precision, {geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532'}, function(err, res){
              should.not.exist(error);
              Object.keys(res).length.should.equal( 17 );
              done();
            });
          });
        });
      });
    });
});
