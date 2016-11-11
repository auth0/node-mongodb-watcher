const MongoClient  = require('mongodb').MongoClient;
const _            = require('lodash');
const MongoWatcher = require('../');
const assert       = require('chai').assert;
const randomstring = require('randomstring');
const sizeOf       = require('object-sizeof');

describe('large.document.fetch', function () {
  var db, collection, watcher, _id;

  before(function(done) {
    MongoClient.connect('mongodb://localhost:27017/mongodb-watcher-tests', function(err, _db) {
      db = _db;
      watcher = new MongoWatcher(db);
      collection = db.collection('large_doc_fetch');
      done();
    });
  });

  before(function (done) {
    collection.remove({}, (err) => {
      if (err) { return done(err); }
      collection.insert({
        test: _.range(1000).map(() => ({ test: randomstring.generate(50) }))
      }, (err, result) => {
        if (err) { return done(err); }
        _id = result.insertedIds[0];
        done();
      });
    });
  });

  after(function() {
    db.close();
  });

  it('should emit an event when retrieving a big document with findOne', function(done) {
    var eventData;

    watcher.once('large.document.fetch', (data) => {
      eventData = data;
    });

    collection.findOne({ _id }, (err, doc) => {
      if (err) { return done(err); }
      assert.isOk(eventData);
      assert.equal(eventData.collection, 'large_doc_fetch');
      assert.equal(eventData.size, sizeOf(doc));
      assert.equal(eventData.documentId, doc._id);
      assert.equal(eventData.cmd.query._id, _id);
      assert.include(eventData.stack, __filename);
      done();
    });
  });

  it('should emit an event when retrieving a big document with find', function(done) {
    var eventData;

    watcher.once('large.document.fetch', (data) => {
      eventData = data;
    });

    collection.find({ _id }).toArray((err, docs) => {
      if (err) { return done(err); }
      assert.isOk(eventData);
      assert.equal(eventData.collection, 'large_doc_fetch');
      assert.equal(eventData.size, sizeOf(docs[0]));
      assert.equal(eventData.documentId, docs[0]._id);
      assert.include(eventData.stack, __filename);
      done();
    });
  });

});
