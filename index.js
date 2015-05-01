var pg = require('pg'),
  ngeohash = require('ngeohash'),
  centroid = require('turf-centroid'),
  sm = require('sphericalmercator'),
  merc = new sm({size:256});

module.exports = {
  geohashPrecision: 8, 
  infoTable: 'koopinfo', 
  timerTable: 'kooptimers',
  limit: 2000,

  connect: function( conn, koop, callback ){
    var self = this;
  
    // use the koop logger 
    this.log = koop.log;
  
    // save the connection string
    this.conn = conn;

    this.client = new pg.Client( conn );
    this.client.connect(function(err) {
      if ( err ){
        console.log('Could not connect to the database');
        process.exit();
      } else {
        // creates table only if they dont exist
        self._createTable(self.infoTable, "( id varchar(255) PRIMARY KEY, info JSON)", null);
        self._createTable(self.timerTable, "( id varchar(255) PRIMARY KEY, expires varchar(25))", null);
      }
      if ( callback ){
        callback();
      }
    });
    return this; 
  },

  // returns the info doc for a key 
  getCount: function( key, options, callback ){
    var self = this;
    var select = 'select count(*) as count from "'+key+'"';
    if ( options.where ){
      if ( options.where != '1=1'){
        var clause = this.createWhereFromSql(options.where);
        select += ' WHERE ' + clause;
      } else {
        select += ' WHERE ' + options.where;
      }
    }

    if ( options.geometry ){
      var geom = this.parseGeometry( options.geometry );

      if ((geom.xmin || geom.xmin === 0) && (geom.ymin || geom.ymin === 0)){
        var box = geom;
        if (box.spatialReference.wkid != 4326){
          var mins = merc.inverse( [box.xmin, box.ymin] ),
            maxs = merc.inverse( [box.xmax, box.ymax] );
          box.xmin = mins[0];
          box.ymin = mins[1];
          box.xmax = maxs[0];
          box.ymax = maxs[1];
        }

        select += (options.where ) ? ' AND ' : ' WHERE ';
        var bbox = box.xmin+' '+box.ymin+','+box.xmax+' '+box.ymax;
        //select += 'geom && ST_SetSRID(\'BOX3D('+bbox+')\'::box3d,4326)';
        select += 'ST_GeomFromGeoJSON(feature->>\'geometry\') && ST_SetSRID(\'BOX3D('+bbox+')\'::box3d,4326)';
      }
    }

    this._query(select, function(err, result){
      if ( err || !result || !result.rows || !result.rows.length ){
        callback('Key Not Found ' + key, null);
      } else {
        self.log.debug('Get Count', result.rows[0].count, select);
        callback(null, parseInt(result.rows[0].count));
      }
    });
  },

  // returns the info doc for a key 
  getInfo: function( key, callback ){
    this._query('select info from "'+this.infoTable+'" where id=\''+key+":info\'", function(err, result){
      if ( err || !result || !result.rows || !result.rows.length ){
        callback('Key Not Found ' + key, null);
      } else {
        var info = result.rows[0].info;
        callback(null, info);
      }
    });
  },

  // updates the info doc for a key 
  updateInfo: function( key, info, callback ){
    this.log.debug('Updating info %s %s', key, info.status);
    this._query("update " + this.infoTable + " set info = '" + JSON.stringify(info) + "' where id = '"+key+":info'", function(err, result){
      if ( err || !result ){
        callback('Key Not Found ' + key, null);
      } else {
        callback(null, true);
      }
    });
  },

  // check for any coded values in the fields
  // if we find a match, replace value with the coded val
  applyCodedDomains: function(fieldName, value, fields){
    fields.forEach(function (field){
      if (field.domain && (field.domain.name && field.domain.name == fieldName)){
        field.domain.codedValues.forEach(function(coded){
          if (parseInt(coded.code) == parseInt(value)){
            value = coded.name;
          }
        });
      }
    });
    return value;
  },

  createRangeFilterFromSql: function (sql, fields) {

    var paramIndex = 0;
    var terms, type;
    if (sql.indexOf(' >= ') > -1) {
      terms = sql.split(' >= ');
      type = '>=';
    } else if (sql.indexOf(' <= ') > -1) {
      terms = sql.split(' <= ');
      paramIndex = 1;
      type = '<=';
    } else if (sql.indexOf(' = ') > -1) {
      terms = sql.split(' = ');
      paramIndex = 1;
      type = '=';
    } else if (sql.indexOf(' > ') > -1){
      terms = sql.split(' > ');
      paramIndex = 1;
      type = '>';
    } else if ( sql.indexOf(' < ') > -1){
      terms = sql.split(' < ');
      paramIndex = 1;
      type = '<';
    }
    if (terms.length !== 2) { return; }

    var fieldName = terms[0].replace(/\'([^\']*)'/g, "$1");
    var value = terms[1];

    // check for fields and apply any coded domains 
    if (fields){
      value = this.applyCodedDomains(fieldName, value, fields);
    }
    //if (dataType === 'date') {
    //  value = moment(value.replace(/(\')|(date \')/g, ''), this.dateFormat).toDate().getTime();
    //}
    var field = ' (feature->\'properties\'->>\''+ fieldName + '\')';
    if ( parseInt(value) || parseInt(value) === 0){
      if ( ( ( parseFloat(value) == parseInt( value ) ) && !isNaN( value )) || value === 0){
        field += '::float::int';
      } else {
        field += '::float';
      }
      return field + ' '+type+' ' + value;
    } else {
      return field + ' '+type+' \'' + value.replace(/'/g, '') + '\'';
    }

  },


  createLikeFilterFromSql: function (sql, fields, dataset) {
    var terms = sql.split(' like ');
    if (terms.length !== 2) { return; }

    // replace N for unicode values so we can rehydrate filter pages
    var value = terms[1].replace(/^N'/g,'\''); //.replace(/^\'%|%\'$/g, '');
    // to support downloads we set quotes on unicode fieldname, here we remove them 
    var fieldName = terms[0].replace(/\'([^\']*)'/g, "$1");
    
    // check for fields and apply any coded domains 
    if (fields){
      value = this.applyCodedDomains(fieldName, value, fields);
    }

    field = ' (feature->\'properties\'->>\''+ fieldName + '\')';
    return field + ' ilike ' + value;
  },

  createFilterFromSql: function (sql, fields) {
    if (sql.indexOf(' like ') > -1) {
      //like
      return this.createLikeFilterFromSql( sql, fields );

    } else if (
      sql.indexOf(' < ') > -1  || 
      sql.indexOf(' > ') > -1  || 
      sql.indexOf(' >= ') > -1 || 
      sql.indexOf(' <= ') > -1 || 
      sql.indexOf(' = ') > -1 )  
    {
      //part of a range
      return this.createRangeFilterFromSql(sql, fields);
    }
  },

  createWhereFromSql: function (sql, fields) {
    var self = this;
    var terms = sql.split(' AND ');
    var pairs, filter, andWhere = [], orWhere = [];

    terms.forEach( function (term) {
      //trim spaces
      term = term.trim();
      //remove parens
      term = term.replace(/(^\()|(\)$)/g, '');
      pairs = term.split(' OR ');
      if ( pairs.length > 1 ){
        pairs.forEach( function (item) {
          orWhere.push( self.createFilterFromSql( item, fields ) );
        });
      } else {
        pairs.forEach( function (item) {
          andWhere.push( self.createFilterFromSql( item, fields ) );
        });
      }
    });
    return andWhere.join(' AND ') + (( orWhere.length ) ? ' AND (' + orWhere.join(' OR ') +')' : '');
  },
  
  // get data out of the db
  select: function(key, options, callback){
    var self = this;
    //var layer = 0;
    var error = false,
      totalLayers,
      queryOpts = {}, 
      allLayers = [];

    // closure to check each layer and send back when done
    var collect = function(err, data){
      if (err) error = err;
      allLayers.push(data);
      if (allLayers.length == totalLayers){
        callback(error, allLayers);
      }
    };
        
    this._query('select info from "'+this.infoTable+'" where id=\''+(key+':'+(options.layer || 0 )+":info")+'\'', function(err, result){
      if ( err || !result || !result.rows || !result.rows.length ){
        callback('Not Found', []);
      } else if (result.rows[0].info.status == 'processing' && !options.bypassProcessing ) {
        callback( null, [{ status: 'processing' }]);
      } else {
          var info = result.rows[0].info;
          var select;
          if (options.simplify){
            select = 'select id, feature->>\'properties\' as props, st_asgeojson(ST_SimplifyPreserveTopology(ST_GeomFromGeoJSON(feature->>\'geometry\'), '+options.simplify+')) as geom from "' + key+':'+(options.layer || 0)+'"'; 
          } else {
            select = 'select id, feature->>\'properties\' as props, feature->>\'geometry\' as geom from "' + key+':'+(options.layer || 0)+'"'; 
          }
  
          // parse the where clause 
          if ( options.where ) { 
            if ( options.where != '1=1'){
              var clause = self.createWhereFromSql(options.where, options.fields);
              select += ' WHERE ' + clause;
            } else {
              select += ' WHERE ' + options.where;
            }
            if (options.idFilter){
              select += ' AND ' + options.idFilter;
            }
          } else if (options.idFilter) {
            select += ' WHERE ' + options.idFilter;
          }

          // parse the geometry param from GeoServices REST
          if ( options.geometry ){
            var geom = self.parseGeometry( options.geometry );

            if ((geom.xmin || geom.xmin === 0) && (geom.ymin || geom.ymin === 0)){
              var box = geom;
              if (box.spatialReference.wkid != 4326){
                var mins = merc.inverse( [box.xmin, box.ymin] ),
                  maxs = merc.inverse( [box.xmax, box.ymax] );
                box.xmin = mins[0];
                box.ymin = mins[1];
                box.xmax = maxs[0];
                box.ymax = maxs[1];
              }

              select += (options.where || options.idFilter) ? ' AND ' : ' WHERE ';
              var bbox = box.xmin+' '+box.ymin+','+box.xmax+' '+box.ymax;
              select += 'ST_GeomFromGeoJSON(feature->>\'geometry\') && ST_SetSRID(\'BOX3D('+bbox+')\'::box3d,4326)';
              //select += 'ST_Intersects(ST_GeomFromGeoJSON(feature->>\'geometry\'), ST_MakeEnvelope('+box.xmin+','+box.ymin+','+box.xmax+','+box.ymax+'))';
            }
          }

          self._query( select.replace(/ id, feature->>'properties' as props, feature->>'geometry' as geom /, ' count(*) as count '), function(err, result){
            if (!options.limit && !err && result.rows.length && (result.rows[0].count > self.limit && options.enforce_limit) ){
              callback( null, [{
                exceeds_limit: true,
                type: 'FeatureCollection',
                features: [{}],
                name: info.name,
                sha: info.sha,
                info: info.info,
                updated_at: info.updated_at,
                retrieved_at: info.retrieved_at,
                expires_at: info.expires_at, 
                count: result.rows[0].count
              }]);

            } else {
              // ensure id order 
              select += " ORDER BY id";
              if ( options.limit ) {
                select += ' LIMIT ' + options.limit;
              }
              if ( options.offset ) {
                select += ' OFFSET ' + options.offset;
              }
              self.log.debug('Selecting data', select);
              self._query( select, function (err, result) {
                if ( result && result.rows && result.rows.length ) {
                  var features = [],
                    feature;
                  result.rows.forEach(function(row, i){
                    features.push({
                      "type": "Feature",
                      "id": row.id,
                      "geometry": JSON.parse(row.geom),
                      "properties": JSON.parse(row.props)
                    });
                  });
                  callback( null, [{
                    type: 'FeatureCollection', 
                    features: features,
                    name: info.name, 
                    sha: info.sha, 
                    info: info.info, 
                    updated_at: info.updated_at,
                    retrieved_at: info.retrieved_at,
                    expires_at: info.expires_at,
                    count: result.rows.length 
                  }]);
                } else {
                  callback( 'Not Found', [{
                    type: 'FeatureCollection',
                    features: []
                  }]);
                }
              });
            }
          });
        }
    });
  },

  parseGeometry: function( geometry ){
    var geom = geometry;
    if ( typeof( geom ) == 'string' ){
      try {
        geom = JSON.parse( geom );
      } catch(e){
        try {
          if ( geom.split(',').length == 4 ){
            var extent = geom.split(',');
            geom = { spatialReference: {wkid: 4326} };
            geom.xmin = extent[0];
            geom.ymin = extent[1];
            geom.xmax = extent[2];
            geom.ymax = extent[3];
          }
        } catch(error){
          this.log.error('Error building bbox from query ' + geometry);
        }
      }
    }
    return geom;
  },

  // create a collection and insert features
  // create a 2d index 
  insert: function( key, geojson, layerId, callback ){
    var self = this; 
    var info = {},
      count = 0;
      error = null;
      
      info.name = geojson.name ;
      info.updated_at = geojson.updated_at;
      info.expires_at = geojson.expires_at;
      info.retrieved_at = geojson.retrieved_at;
      info.status = geojson.status;
      info.format = geojson.format;
      info.sha = geojson.sha;
      info.info = geojson.info;
      info.host = geojson.host;
   
      var table = key+':'+layerId;

      var feature = (geojson[0]) ? geojson[0].features[0] : geojson.features[0];
      
      var types = {
        'esriGeometryPolyline': 'LineString',
        'esriGeometryPoint': 'Point',
        'esriGeometryPolygon': 'Polygon'
      };

      if (!feature){
        feature = { geometry: { type: geojson.geomType || types[geojson.info.geometryType] } };
      }

      self._createTable( table, self._buildSchemaFromFeature(feature), true, function(err, result){
        if (err){
          callback(err, false);
          return;
        }

        // insert each feature
        if ( geojson.length ){
          geojson = geojson[0];
        }
        geojson.features.forEach(function(feature, i){
          self._query(self._insertFeature(table, feature, i), function(err){ 
            if (err) {
              self.log.error(err); 
            }
          });
        });

        // TODO Why not use an update query here? 
        self._query( 'delete from "'+self.infoTable+'" where id=\''+table+':info\'', function(err,res){
          self._query( 'insert into "'+self.infoTable+'" values (\''+table+':info\',\''+JSON.stringify(info).replace(/'/g,'')+'\')', function(err, result){
            callback(err, true);
          });
        });
      });     
    
  },

  insertPartial: function( key, geojson, layerId, callback ){
    var self = this;
    var info = {};

    var sql = 'BEGIN;';
    var table = key+':'+layerId;
    geojson.features.forEach(function(feature, i){
        sql += self._insertFeature(table, feature, i);
    });
    sql += 'COMMIT;';
    this._query(sql, function(err, res){
      if (err){
        self.log.error('insert partial ERROR %s, %s', err, key);
        self._query('ROLLBACK;', function(){
          callback(err, false);
        });
      } else {
        self.log.debug('insert partial SUCCESS %s', key);
        callback(null, true);
      }
    });
  },

  // inserts geojson features into the feature column of the given table
  _insertFeature: function(table, feature, i){
    var featureString = JSON.stringify(feature).replace(/'/g, "");

    if (feature.geometry && feature.geometry.coordinates && feature.geometry.coordinates.length ){
      var geohash = this.createGeohash(feature, this.geohashPrecision);
      feature.geometry.crs = {"type":"name","properties":{"name":"EPSG:4326"}};
      return 'insert into "'+table+'" (feature, geohash) VALUES (\''+featureString+'\', \''+geohash+'\');';
    } else {
      return 'insert into "'+table+'" (feature) VALUES (\''+featureString+'\');' ;
    }
  },

  createGeohash: function (feature, precision) {
    if (!feature.geometry || !feature.geometry.coordinates) {
      return;
    }
    if (feature.geometry.type !== 'Point') {
      feature = centroid(feature);
    }
    var pnt = feature.geometry.coordinates;
    return ngeohash.encode(pnt[1], pnt[0], precision);
  },

  remove: function( key, callback){
    var self = this;
    
    this._query('select info from "'+this.infoTable+'" where id=\''+(key+":info")+"'", function(err, result){
      if ( !result || !result.rows.length ){
        // nothing to remove
        callback( null, true );
      } else {
        var info = result.rows[0].info;
        self.dropTable(key, function(err, result){
            self._query("delete from \""+self.infoTable+"\" where id='"+(key+':info')+"'", function(err, result){
              if (callback) callback( err, true);
            });
        });
      }
    });
  },

  dropTable: function(table, callback){
    this._query('drop table "'+table+'"' , callback);
  },

  serviceRegister: function( type, info, callback){
      var self = this;
      this._createTable(type, '( id varchar(100), host varchar(100))', null, function(err, result){
        self._query('select * from "'+type+'" where id=\''+info.id+"\'", function(err, res){
          if ( err || !res || !res.rows || !res.rows.length ) {
            var sql = 'insert into "'+type+'" (id, host) VALUES (\''+info.id+'\', \''+info.host+'\')' ;
            self._query(sql, function(err, res){
              callback( err, true );
            });
          } else {
            callback( err, true );
          }
        });  
      });
  },

  serviceCount: function( type, callback){
      var sql = 'select count(*) as count from "'+type+'"';
      this._query(sql, function(err, res){
        if (err || !res || !res.rows || !res.rows.length){
          callback( err, 0 );
        } else {
          callback( err, res.rows[0].count );
        }
      });
  },

  serviceRemove: function( type, id, callback){
      var sql = 'delete from "'+type+'" where id=\''+id+"'";
      this._query(sql, function(err, res){
        callback( err, true );
      });
  },

  serviceGet: function( type, id, callback){
      var self = this;
      var sql;
      //this._createTable(type, '( id varchar(100), host varchar(100))', null, function(err, result){
        if (!id) {
          sql = 'select * from "'+type+'"';
          self._query(sql, function(err, res){
            callback( err, (res) ? res.rows : null);
          });
        } else {
          sql = 'select * from "'+type+'" where id=\''+id+"\'";
          self._query(sql, function(err, res){
            if (err || !res || !res.rows || !res.rows.length){
              err = 'No service found by that id';
              callback( err, null);
            } else {
              callback( err, res.rows[0]);
            }
          });
        }
      //});
  },

  timerSet: function(key, expires, callback){
      var self = this;
      var now = new Date();
      var expires_at = new Date( now.getTime() + expires );
      this._query('delete from "'+ this.timerTable +'" WHERE id=\''+key+"\'", function(err,res){
        self._query('insert into "'+ self.timerTable +'" (id, expires) VALUES (\''+key+'\', \''+expires_at.getTime()+'\')', function(err, res){
          callback( err, res);
        });
      });
    },

  timerGet: function(key, callback){
    this._query('select * from "'+ this.timerTable + '" where id=\''+key+"\'", function(err, res){
      if (err || !res || !res.rows || !res.rows.length ){
        callback( err, null);
      } else {
        if ( new Date().getTime() < parseInt( res.rows[0].expires )){
          callback( err, res.rows[0]);
        } else {
          callback( err, null);
        }
      }
    });
  },

  isNumeric: function(num) {
     return (num >=0 || num < 0);
  },

  geoHashAgg: function (table, limit, precision, options, callback) {
    var self = this, 
      rowLimit = options.rowLimit || 10000, 
      pageLimit = options.pageLimit || 5000;

    options.whereFilter = null, 
    options.geomFilter = null;

    // parse the where clause 
    if ( options.where ){
      if ( options.where != '1=1'){
        var clause = this.createWhereFromSql(options.where);
        options.whereFilter = ' WHERE ' + clause;
      } else {
        options.whereFilter = ' WHERE ' + options.where;
      } 
    }

    // parse the geometry into a bbox 
    if ( options.geometry ){
      var geom = this.parseGeometry( options.geometry );
      // make sure each coord is numeric
      if (this.isNumeric(geom.xmin) && 
        this.isNumeric(geom.xmax) && 
        this.isNumeric(geom.ymin) && 
        this.isNumeric(geom.ymax)
      ){
        var box = geom;
        if (box.spatialReference.wkid != 4326){
          var mins = merc.inverse( [box.xmin, box.ymin] ),
            maxs = merc.inverse( [box.xmax, box.ymax] );
          box.xmin = mins[0];
          box.ymin = mins[1];
          box.xmax = maxs[0];
          box.ymax = maxs[1];
        }

        var bbox = box.xmin+' '+box.ymin+','+box.xmax+' '+box.ymax;
        options.geomFilter = ' ST_GeomFromGeoJSON(feature->>\'geometry\') && ST_SetSRID(\'BOX3D('+bbox+')\'::box3d,4326)';
      }
    }

    // recursively get geohash counts until we have a precision 
    // that reutrns less than the row limit
    // this will return the precision that will return the number 
    // of geohashes less than the limit
    var reducePrecision = function(table, p, options, callback){
      self.countDistinctGeoHash(table, p, options, function(err, count){
        console.log(count, limit, p)
        if (parseInt(count, 0) > limit) {
          reducePrecision(table, p-1, options, callback);
        } else {
          callback(err, p);
        }
      });
    };

    var agg = {};
    reducePrecision(table, precision, options, function (err, newPrecision) {
      var geoHashSelect;
      if (newPrecision < precision) {
        geoHashSelect = 'substring(geohash,0,'+(newPrecision+1)+')';
      } else {
        geoHashSelect = 'geohash';
      }
      var sql = 'SELECT count(id) as count, '+geoHashSelect+' as geohash from "'+table+'"';

      // apply any filters to the sql
      if (options.whereFilter) {
        sql += options.whereFilter;
      }
      if (options.geomFilter){
        sql += ((options.whereFilter) ? ' AND ' : ' WHERE ') + options.geomFilter;
      }
      sql += ' GROUP BY '+geoHashSelect;
      self._query(sql, function(err, res){
        if (!err && res && res.rows.length) {
            console.log(sql, res.rows.length)
            res.rows.forEach(function (row) {
              console.log
              agg[row.geohash] = row.count;
            });
            callback(err, agg);
        } else {
          callback(err, res);
        }
      });
    }); 

      // get the count of features
      /*self._query( countSql, function(err, res){
        if (!err && res.rows.length && (res.rows[0].count > rowLimit)) {
          // count is too high, must page over the data 
          var npages = Math.ceil(res.rows[0].count/ pageLimit);
          var tasks = [];
          for (var i=0; i < npages; i++){
            var offset = (i * (pageLimit)) + 1;
            var filter = ' id >= '+ offset + ' AND id < ' + (offset + pageLimit);
            tasks.push(sql + ((options.whereFilter || options.geomFilter) ? ' AND '+filter : ' WHERE '+filter) + ' GROUP BY geohash');
          }
          callback(null, {
            pages: tasks,
            status: 'exceeds-limit' 
          });
        } else {        
          
          sql += ' GROUP BY geohash';
          self._query(sql, function(err, res){
            if (!err && res && res.rows.length) {
              if (res.rows.length <= limit) {
                res.rows.forEach(function (row) {
                  agg[row.geohash] = row.count;
                });
                callback(err, agg);
              } else {
                build(p-1);
              }
            } else {
              callback(err, res);
            }
          });
        } 
      });*/
    //}; 
  },

  // Get the count of distinct geohashes for a query 
  countDistinctGeoHash: function(table, precision, options, callback){
    var countSql = 'select count(DISTINCT(substring(geohash,0,'+precision+'))) as count from "'+table+'"';
    // apply any filters to the sql
    if (options.whereFilter) {
      countSql += options.whereFilter;
    }
    if (options.geomFilter) {
      countSql += ((options.whereFilter) ? ' AND ' : ' WHERE ') + options.geomFilter;
    }
    console.log(countSql);
    this._query( countSql, function (err, res) {
      callback(err, res.rows[0].count);
    });
    
  },

  buildGeoHashPages: function(sql, idFilters, options, callback){
    var self = this;
    var pageSql = []
    idFilters.forEach(function (filter) {
      pageSql.push(sql + ((options.whereFilter || options.geomFilter) ? ' AND '+filter : ' WHERE '+filter) + ' GROUP BY geohash'); 
    });
    callback('Not ready yet', null);
  },

  getGeoHashPage: function(sql, callback){
    var agg = {};
    this._query(sql, function(err, res){
      if (!err && res && res.rows.length) {
        res.rows.forEach(function (row) {
          agg[row.geohash] = row.count;
        });
        callback(err, agg);
      } else {
        callback(err, res);
      }
    });
  }, 

  //--------------
  // PRIVATE METHODS
  //-------------

  _query: function(sql, callback){
    pg.connect(this.conn, function(err, client, done) {
      if(err) {
        return console.error('!error fetching client from pool', err);
      }
      client.query(sql, function(err, result) {
        //call `done()` to release the client back to the pool
        done();
        if ( callback ) {
          callback(err, result);
        }
      });
    });

  },


  // checks to see in the info table exists, create it if not
  _createTable: function(name, schema, index, callback){
    var self = this;
    var sql = "select exists(select * from information_schema.tables where table_name='"+ name +"')";
    this._query(sql, function(err, result){
      if ( result && !result.rows[0].exists ){
        var create = "CREATE TABLE \"" + name + "\" " + schema;
        self.log.info(create);
        self._query(create, function(err, result){
            if (err){
              callback('Failed to create table '+name);
              return;
            }

            if ( index ){
              self._query( 'CREATE INDEX '+name.replace(/:|-/g,'')+'_gix ON "'+name+'" USING GIST ( ST_GeomfromGeoJSON(feature->>\'geometry\') )', function(err){
                if (callback) {
                  callback();
                }
              });
            } else {
              if (callback) {
                callback();
              }
            }
        });
      } else if (callback){
        callback();
      }
    });
  },

  _buildSchemaFromFeature: function(feature){
    var schema = '(';
    var type; 
    if (feature && feature.geometry && feature.geometry.type ){
      type = feature.geometry.type.toUpperCase();
    } else {
      // default to point geoms
      type = 'POINT';
    }
    var props = ['id SERIAL PRIMARY KEY', 'feature JSON', 'geom Geometry('+type+', 4326)', 'geohash varchar(10)'];
    schema += props.join(',') + ')';
    return schema;
  }

};
