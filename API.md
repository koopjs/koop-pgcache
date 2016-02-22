# API

## Initialize
```javascript
Const cache = new Cache(options)
cache.connect()
cache.on('connect')
cache.on('disconnect')
cache.on('error')
cache.on('insert')
cache.on('promotion')
cache.on('update')
```

## Table

### Create a new table
```javascript
Cache.Table.create(id, options, callback)
```

### Drop an existing table
```javascript
Cache.Table.drop(id, callback)
```

### Replace an existing table with another existing table
```javascript
Cache.Table.promote(old, new, callback)
```

### Add indexes to an existing table
```javascript
Cache.Table.addIndexes(id, options, callback)
```

## Info

### Insert a row into the info table
```javascript
Cache.Info.insert(id, info, callback)
```

### Update a row in the info table
```javascript
Cache.Info.update(id, info, callback)
```

### Delete a row from the info table
```javascript
Cache.Info.delete(id, callback)
```

## Features

### Insert features into the cache
```javascript
Cache.Features.insert(id, features, callback)
```

### Select features from the cache
```javascript
Cache.Features.select(id, query, callback)
```

### Create a stream of features from the cache
```javascript
Cache.Features.createStream(id, query)
```

## Aggregate

### Create a geohash aggregation
```javascript
Cache.Aggregate.geohash(id, query, callback)
```

### Calculate statistics for a group of features
```javascript
Cache.Aggregate.statistics(id, query, options, callback)
```

### Calculate the extent for a group of features
```javascript
Cache.Aggregate.extent(id, query, callback)
```

## Hosts

### Insert a row into hosts table
```javascript
Cache.Hosts.insert(source, id, url, callback)
```
### Select a row from the hosts table
```javascript
Cache.Hosts.select(source, id, callback)
```

### Delete a row from the hosts table
```javascript
Cache.Hosts.delete(source, id, callback)
```

## Spatial Reference Systems

### Select a wkt
```javascript
Cache.SRS.select(srid, callback)
```

### Insert a WKT
```javascript
Cache.SRS.insert(srid, wkt, callback)
```
