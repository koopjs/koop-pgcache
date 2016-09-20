/* global describe, it */
var should = require('should')
var ExportStream = require('../lib/exportStream')

describe('When creating an export stream', function () {
  it('should emit an error when postgres complains', function (done) {
    ExportStream.create('foo', 'foo', {})
    .on('error', function (err) {
      should.exist(err)
      done()
    })
    .map(function (features) {
      throw new Error('Fail')
    })
  })
})
