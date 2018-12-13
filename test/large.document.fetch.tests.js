const MongoClient  = require('mongodb').MongoClient;
const MongoWatcher = require('../');
const assert       = require('chai').assert;
const sizeOf       = require('object-sizeof');

describe('large.document.fetch', function () {
  var db, collection, watcher, _id;

  function setupDb(cb) {
    MongoClient.connect('mongodb://localhost:27017/mongodb-watcher-tests', function(err, _db) {
      if (err) { return cb(err); }
      db = _db;
      cb();
    });
  }

  function setUpWatcher(params, cb) {
    setupDb(function(err) {
      if (err) { return cb(err); }
      watcher = new MongoWatcher(db, params);
      collection = db.collection('large_doc_fetch');
      collection.remove({}, cb);
    });
  }

  const removeEventListeners = () => {
    if (watcher) {
      watcher.removeAllListeners();
    }
  };
  const cleanCollection = done => collection.remove({}, done);
  const buildObjectSlightlyBiggerThan = size => ({ id: 'obj', test: { withArray: [ 'a'.repeat(size/2) ] }}); // Each char is 2 bytes
  const createObjectInsertionBySize = size => done => collection.insert(buildObjectSlightlyBiggerThan(size), done);

  const save_idAfterInsert = done => (err, result) => {
    if (err) { return done(err); }
    _id = result.insertedIds[0];
    done();
  };

  before(setupDb);

  after(function() {
    db.close();
  });

  const defaultThreshold = 30 * 1024;
  const customThreshold = 2 * 1024;

  [
    {
      description: 'default threshold',
      threshold: defaultThreshold,
    },
    {
      description: 'custom threshold',
      threshold: customThreshold,
      watcherParams: {
        largeFetchThreshold: customThreshold
      }
    },
  ].forEach(function(testCase){

    describe(testCase.description, function(){
      before(done => setUpWatcher(testCase.watcherParams, done));
      afterEach(removeEventListeners);

      describe('when object is bigger than threshold', function(){
        before(done => {
          const insert = createObjectInsertionBySize(testCase.threshold);
          insert(save_idAfterInsert(done));
        });
        after(cleanCollection);

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

        it('should emit an event when retrieving a big document with find + toArray', function(done) {
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

      describe('when object is smaller than threshold', function(){
        before(done => {
          const insert = createObjectInsertionBySize(testCase.threshold / 10);
          insert(save_idAfterInsert(done));
        });
        after(cleanCollection);

        it('should not emit any event when retrieving a big document with findOne', function(done) {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          collection.findOne({ _id }, (err, doc) => {
            if (err) { return done(err); }
            assert.isUndefined(eventData);
            assert.isOk(doc);
            done();
          });
        });

        it('should not emit any event when retrieving a big document with find + toArray', function(done) {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          collection.find({ _id }).toArray((err, docs) => {
            if (err) { return done(err); }
            assert.isUndefined(eventData);
            assert.isOk(docs);
            done();
          });
        });
      });
    });
  });

  describe('when a custom check interval is used', function(){

    describe('input validation', function(){
      [
        null,
        undefined,
        -1,
        0,
        Number.MAX_VALUE,
        ['an-array'],
        {},
        'string'
      ].forEach(function(interval){
        it('should error for invalid value: ' + interval, function(){
          assert.throw(() => new MongoWatcher(db, { largeFetchCheckInterval: interval }), /Interval must be a positive save integer, found: .*/);
        });
      });
    });

    describe('event interval', function(){
      beforeEach(done => setUpWatcher({ largeFetchCheckInterval: 3 }, done));
      beforeEach(done => {
        const obj1 = buildObjectSlightlyBiggerThan(defaultThreshold);
        const obj2 = buildObjectSlightlyBiggerThan(defaultThreshold);
        const obj3 = buildObjectSlightlyBiggerThan(defaultThreshold);
        const obj4 = buildObjectSlightlyBiggerThan(defaultThreshold);
        collection.insertMany([obj1, obj2, obj3, obj4], done);
      });
      afterEach(removeEventListeners);
      after(cleanCollection);

      it('should emit only the first time of each interval when retrieving a big document with findOne', function(done){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        collection.findOne({ id: 'obj' }, (err, doc) => {
          if (err) { return done(err); }
          assert.lengthOf(events, 1);
          assert.isOk(doc);

          collection.findOne({ id: 'obj' }, (err, doc) => {
            if (err) { return done(err); }
            assert.lengthOf(events, 1);
            assert.isOk(doc);

            collection.findOne({ id: 'obj' }, (err, doc) => {
              if (err) { return done(err); }
              assert.lengthOf(events, 1);
              assert.isOk(doc);

              collection.findOne({ id: 'obj' }, (err, doc) => {
                if (err) { return done(err); }
                assert.lengthOf(events, 2);
                assert.isOk(doc);
                done();
              });
            });
          });
        });
      });

      it('should emit the first time when retrieving a big document with find + toArray', function(done){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        collection.find({ id: 'obj' }).toArray((err, docs) => {
          if (err) { return done(err); }
          assert.lengthOf(docs, 4);
          // interval: X--X <- here, got 2 events ("X" means event sent)
          assert.lengthOf(events, 2);

          collection.find({ id: 'obj' }).toArray((err, docs) => {
            if (err) { return done(err); }
            assert.lengthOf(docs, 4);
            // interval: X--X--X- <- here, got 1 more events
            assert.lengthOf(events, 3);

            collection.find({ id: 'obj' }).toArray((err, docs) => {
              if (err) { return done(err); }
              assert.lengthOf(docs, 4);
              // interval: X--X--X--X-- <- here, got 1 more events
              assert.lengthOf(events, 4);

              collection.find({ id: 'obj' }).toArray((err, docs) => {
                if (err) { return done(err); }
                assert.lengthOf(docs, 4);
                // interval: X--X--X--X--X--X <- here, got 2 more events
                assert.lengthOf(events, 6);
                done();
              });
            });
          });
        });
      });

      it('should emit the first time when retrieving a big document mixing findOne and find + toArray', function(done){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        collection.find({ id: 'obj' }).toArray((err, docs) => {
          if (err) { return done(err); }
          assert.lengthOf(docs, 4);
          // interval: X--X <- here, got 2 events ("X" means event sent)
          assert.lengthOf(events, 2);

          collection.findOne({ id: 'obj' }, (err, doc) => {
            if (err) { return done(err); }
            assert.isOk(doc);
            // interval: X--X- <- here, got none events
            assert.lengthOf(events, 2);

            collection.find({ id: 'obj' }).toArray((err, docs) => {
              if (err) { return done(err); }
              assert.lengthOf(docs, 4);
              // interval: X--X--X-- <- here, got 1 more event
              assert.lengthOf(events, 3);

              collection.findOne({ id: 'obj' }, (err, doc) => {
                if (err) { return done(err); }
                assert.isOk(doc);
                // interval: X--X--X--X <- here, got 1 more event
                assert.lengthOf(events, 4);
                done();
              });
            });
          });
        });
      });
    });
  });
});
