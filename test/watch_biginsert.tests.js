const MongoClient  = require('mongodb').MongoClient;
const _            = require('lodash');
const MongoWatcher = require('../');
const assert       = require('chai').assert;
const randomstring = require('randomstring');
const sizeOf       = require('object-sizeof');

describe('big insert', function () {
  var db, collection, watcher;

  before(function(done) {
    MongoClient.connect('mongodb://localhost:27017/mongodb-watcher-tests', function(err, _db) {
      db = _db;
      watcher = new MongoWatcher(db);
      collection = db.collection('biginsert');
      done();
    });
  });

  before(function (done) {
    collection.remove({}, done);
  });

  after(function() {
    db.close();
  });

  it('should emit an event when inserting a big document', function(done) {
    const badDoc = {
      test: _.range(1000).map(() => ({ test: randomstring.generate(50) }))
    };

    watcher.once('big insert', (data) => {
      assert.isOk(badDoc._id);
      assert.equal(data.collection, 'biginsert');
      assert.equal(data.size, sizeOf(badDoc));
      assert.equal(data.documentId, badDoc._id);
      assert.include(data.stack, 'test/watch_biginsert.tests.js');
      done();
    });

    collection.insert(badDoc, _.noop);
  });

  it('should emit an event when saving an big document', function(done) {
    const badDoc = {
      test: _.range(1000).map(() => ({ test: randomstring.generate(50) }))
    };

    watcher.once('big insert', (data) => {
      assert.isOk(badDoc._id);
      assert.equal(data.collection, 'biginsert');
      assert.equal(data.size, sizeOf(badDoc));
      assert.equal(data.documentId, badDoc._id);
      assert.include(data.stack, 'test/watch_biginsert.tests.js');
      done();
    });

    collection.save(badDoc, _.noop);
  });

  it('should not emit the event when the inserting an small document', function(done) {
    const goodDoc = {
      "foo": "bar"
    };

    var emitted = false;
    watcher.once('big insert', () => {
      emitted = true;
      done(new Error('this should not be emitted the document is ' + sizeOf(goodDoc)));
    });

    collection.insert(goodDoc, () => {
      setTimeout(() => {
        if (!emitted) done();
      }, 10);
    });
  });

  it('should not emit the event when the saving an small document', function(done) {
    const goodDoc = {
      "foo": "bar"
    };

    var emitted = false;
    watcher.once('big insert', () => {
      emitted = true;
      done(new Error('this should not be emitted the document is ' + sizeOf(goodDoc)));
    });

    collection.save(goodDoc, () => {
      setTimeout(() => {
        if (!emitted) done();
      }, 10);
    });
  });
});
