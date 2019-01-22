const MongoClient  = require('mongodb').MongoClient;
const MongoWatcher = require('../');
const assert       = require('chai').assert;
const sizeOf       = require('object-sizeof');

describe('large.document.fetch (returning promises)', function () {
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
  const cleanCollection = done => { collection.remove({}, done); };
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

        it('should emit an event when retrieving a big document with findOne', function() {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          return collection.findOne({ _id }).then((doc) => {
            assert.isOk(eventData);
            assert.equal(eventData.collection, 'large_doc_fetch');
            assert.equal(eventData.size, sizeOf(doc));
            assert.equal(eventData.documentId, doc._id);
            assert.equal(eventData.cmd.query._id, _id);
            assert.include(eventData.stack, __filename);
          });
        });

        it('should emit an event when retrieving a big document with find + toArray', function() {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          return collection.find({ _id }).toArray().then((docs) => {
            assert.isOk(eventData);
            assert.equal(eventData.collection, 'large_doc_fetch');
            assert.equal(eventData.size, sizeOf(docs[0]));
            assert.equal(eventData.documentId, docs[0]._id);
            assert.include(eventData.stack, __filename);
          });
        });
      });

      describe('when object is smaller than threshold', function(){
        before(done => {
          const insert = createObjectInsertionBySize(testCase.threshold / 10);
          insert(save_idAfterInsert(done));
        });
        after(cleanCollection);

        it('should not emit any event when retrieving a big document with findOne', function() {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          return collection.findOne({ _id }).then((doc) => {
            assert.isUndefined(eventData);
            assert.isOk(doc);
          });
        });

        it('should not emit any event when retrieving a big document with find + toArray', function() {
          var eventData;

          watcher.once('large.document.fetch', (data) => {
            eventData = data;
          });

          return collection.find({ _id }).toArray().then((docs) => {
            assert.isUndefined(eventData);
            assert.isOk(docs);
          });
        });
      });
    });
  });

  describe('when a custom check interval is used', function(){

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

      it('should emit only the first time of each interval when retrieving a big document with findOne', function(){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        return collection.findOne({ id: 'obj' }).then((doc) => {
          assert.lengthOf(events, 1);
          assert.isOk(doc);

          return collection.findOne({ id: 'obj' }).then((doc) => {
            assert.lengthOf(events, 1);
            assert.isOk(doc);

            return collection.findOne({ id: 'obj' }).then((doc) => {
              assert.lengthOf(events, 1);
              assert.isOk(doc);

              return collection.findOne({ id: 'obj' }).then((doc) => {
                assert.lengthOf(events, 2);
                assert.isOk(doc);
              });
            });
          });
        });
      });

      it('should emit the first time when retrieving a big document with find + toArray', function(){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        return collection.find({ id: 'obj' }).toArray().then((docs) => {
          assert.lengthOf(docs, 4);
          // interval: X--X <- here, got 2 events ("X" means event sent)
          assert.lengthOf(events, 2);

          return collection.find({ id: 'obj' }).toArray().then((docs) => {
            assert.lengthOf(docs, 4);
            // interval: X--X--X- <- here, got 1 more events
            assert.lengthOf(events, 3);

            return collection.find({ id: 'obj' }).toArray().then((docs) => {
              assert.lengthOf(docs, 4);
              // interval: X--X--X--X-- <- here, got 1 more events
              assert.lengthOf(events, 4);

              return collection.find({ id: 'obj' }).toArray().then((docs) => {
                assert.lengthOf(docs, 4);
                // interval: X--X--X--X--X--X <- here, got 2 more events
                assert.lengthOf(events, 6);
              });
            });
          });
        });
      });

      it('should emit the first time when retrieving a big document mixing findOne and find + toArray', function(){
        const events = [];

        watcher.on('large.document.fetch', (data) => {
          events.push(data);
        });

        return collection.find({ id: 'obj' }).toArray().then((docs) => {
          assert.lengthOf(docs, 4);
          // interval: X--X <- here, got 2 events ("X" means event sent)
          assert.lengthOf(events, 2);

          return collection.findOne({ id: 'obj' }).then((doc) => {
            assert.isOk(doc);
            // interval: X--X- <- here, got none events
            assert.lengthOf(events, 2);

            return collection.find({ id: 'obj' }).toArray().then((docs) => {
              assert.lengthOf(docs, 4);
              // interval: X--X--X-- <- here, got 1 more event
              assert.lengthOf(events, 3);

              return collection.findOne({ id: 'obj' }).then((doc) => {
                assert.isOk(doc);
                // interval: X--X--X--X <- here, got 1 more event
                assert.lengthOf(events, 4);
              });
            });
          });
        });
      });
    });
  });
});
