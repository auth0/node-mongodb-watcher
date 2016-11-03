const MongoClient  = require('mongodb').MongoClient;
const _            = require('lodash');
const async        = require('async');
const MongoWatcher = require('../');
const assert       = require('chai').assert;

describe('long cursors', function () {
  var db, collection, watcher;

  before(function(done) {
    MongoClient.connect('mongodb://localhost:27017/mongodb-watcher-tests', function(err, _db) {
      db = _db;
      collection = db.collection('longcursors');
      watcher = new MongoWatcher(db);
      done();
    });
  });

  before(function (done) {
    async.series([
      cb => collection.remove({}, cb),
      cb => collection.insert(_.range(1000).map(i => ({ test: i })), cb)
    ], done);
  });

  after(function() {
    db.close();
  });

  it('should emit an event if query is going to return more than 100 documents', function(done) {
    watcher.once('long cursor', (data) => {
      assert.equal(data.collection, 'longcursors');
      assert.equal(data.count, 500);
      assert.equal(data.cmd.query.notFoo.$exists, false);
      assert.include(data.stack, 'test/watch_longcursors.tests.js');
      done();
    });
    collection.find({ notFoo: { $exists: false} }).limit(500).toArray(_.noop);
  });

  it('should not emit an event if query is going to return less than 100 documents', function(done) {
    watcher.once('long cursor', () => {
      done(new Error('this should not be called'));
    });
    collection.find({}).limit(50).toArray(done);
  });
});
