const MongoClient  = require('mongodb').MongoClient;
const _            = require('lodash');
const MongoWatcher = require('../');
const assert       = require('chai').assert;

describe('long insert', function () {
  var db, collection, watcher;

  before(function(done) {
    MongoClient.connect('mongodb://localhost:27017/mongodb-watcher-tests', function(err, _db) {
      db = _db;
      watcher = new MongoWatcher(db);
      collection = db.collection('longinsert');
      done();
    });
  });

  before(function (done) {
    collection.remove({}, done);
  });

  after(function() {
    db.close();
  });

  it('should emit an event when inserting more than 100 documents', function(done) {
    watcher.once('long insert', (data) => {
      assert.equal(data.collection, 'longinsert');
      assert.equal(data.count, 1000);
      assert.include(data.stack, 'test/watch_longinsert.tests.js');
      done();
    });
    collection.insert(_.range(1000).map(i => ({ test: i })), _.noop);
  });

  it('should work when getting the collection with callback', function(done) {
    watcher.once('long insert', (data) => {
      assert.equal(data.collection, 'longinsert');
      assert.equal(data.count, 1000);
      assert.include(data.stack, 'test/watch_longinsert.tests.js');
      done();
    });
    db.collection('longinsert', function(err, collection) {
      if (err) { return done(err); }
      collection.insert(_.range(1000).map(i => ({ test: i })), _.noop);
    });
  });

  it('should not emit an event when inserting less than 100 documents', function(done) {
    watcher.once('long insert', () => {
      done(new Error('should not be called'));
    });
    collection.insert(_.range(10).map(i => ({ test: i })), done);
  });

  it('should not emit an event when inserting one document', function(done) {
    watcher.once('long insert', () => {
      done(new Error('should not be called'));
    });
    collection.insert({test: 123}, done);
  });
});
