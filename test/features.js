const test = require('tape')
const Cache = require('../src')
const cache = new Cache()
const features = cache.features
const snowData = require('./fixtures/snow.geojson')
const exec = require('child_process').exec
const path = require('path')

test('Set up', t => {
  exec('dropdb koopdev', err => {
    if (err) console.log(err)
    exec('createdb koopdev', err => {
      if (err) console.log(err)
      if (err) process.exit(1)
      exec(`psql koopdev < ${path.join(__dirname, 'db.sql')}`, err => {
        if (err) process.exit(1)
        t.end()
      })
    })
  })
})

test('Insert features into an existing table', t => {
  t.plan(2)
  features.insert('test:table:2', snowData, (err, success) => {
    if (err) return t.end()
    features.select('test:table:2', {}, (error, featureCollection) => {
      if (error) console.log(error)
      if (error) return t.end()
      t.ok(featureCollection.features)
      t.equal(featureCollection.features.length, snowData.features.length)
    })
  })
})

test('Select features from the db', t => {
  t.plan(2)
  features.select('test:table:1', {}, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 417)
  })
})

test('Select features from the db with a where filter', t => {
  t.plan(2)
  features.select('test:table:1', { where: '\'total precip\' = \'0.31\'' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 5)
  })
})

test('Select features from the db with an OR filter', t => {
  t.plan(2)
  features.select('test:table:1', { where: '\'total precip\' = \'0.31\' OR \'total precip\' > \'0.5\'' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 120)
  })
})

test('Select features from the DB with an Order By parameter', t => {
  t.plan(2)
  features.select('test:table:1', { limit: 1, order_by: [{'total precip': 'DESC'}] }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features[0])
    t.equal(featureCollection.features[0].properties['total precip'], 1.5)
  })
})

test('Select features from the DB with an AND condition', t => {
  t.plan(2)
  features.select('test:table:0', { where: 'ID >= 2894 AND ID <= \'2997\'' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 7)
  })
})

test('Select data with many AND filters', t => {
  t.plan(2)
  features.select('test:table:0', { where: 'ID >= 2894 AND ID <= 2997 AND Land like \'%germany%\' AND Art like \'%BRL%\'' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 2)
  })
})

test('Select data with OR and AND filters', t => {
  t.plan(2)
  features.select('test:table:0', { where: 'ID >= 2894 AND ID <= 3401 AND  (Land = \'Germany\' OR Land = \'Poland\')  AND Art = \'BRL\'' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 5)
  })
})

test('Select data with a geometry filter', t => {
  t.plan(2)
  features.select('test:table:0', { geometry: '11.296916335529545,50.976109119993865,14.273970437121521,52.39566469623532' }, (error, featureCollection) => {
    if (error) return t.end()
    t.ok(featureCollection.features)
    t.equal(featureCollection.features.length, 26)
  })
})

test('Stream features from the db', t => {
  t.plan(1)
  const exportStream = features.createStream('test:table:1', {json: true})
  exportStream.toArray(features => t.equal(features.length, 417))
})

test('Stream filtered data from the db', t => {
  t.plan(1)
  const exportStream = features.createStream('test:table:1', {where: '\'total precip\' = \'0.31\'', geometry: '-180,90,180,-90'})
  exportStream.toArray(features => {
    t.equal(features.length, 5)
    exportStream.disconnect()
  })
})

test('Teardown', t => {
  t.end()
})
