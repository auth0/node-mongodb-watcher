const MongoClient  = require('mongodb').MongoClient;
const async        = require('async');
const MongoWatcher = require('../');
const assert       = require('chai').assert;

describe('long.cursor.enumeration', function () {
  var db, collection, watcher;

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
      collection = db.collection('longcursors');
      collection.remove({}, cb);
    });
  }

  const removeEventListeners = () => {
    if (watcher) {
      watcher.removeAllListeners();
    }
  };
  const cleanCollection = done => collection.remove({}, done);
  const insertDocs = howMany => cb => collection.insert(new Array(howMany).fill().map(_ => ({ test: 't' })), cb);

  before(setupDb);

  after(function() {
    db.close();
  });

  const defaultThreshold = 100;
  const customThreshold = 20;

  [
    {
      description: 'default threshold',
      threshold: defaultThreshold,
    },
    {
      description: 'custom threshold',
      threshold: customThreshold,
      watcherParams: {
        longCursorThreshold: customThreshold
      }
    },
  ].forEach(function(testCase){

    describe(testCase.description, function(){
      before(function(done){
        setUpWatcher(testCase.watcherParams, done);
      });
      afterEach(cleanCollection);
      afterEach(removeEventListeners);

      describe('when document list is longer than threshold', function(){

        before(insertDocs(testCase.threshold + 1));

        it('should emit an event', function(done) {
          watcher.once('long.cursor.enumeration', (data) => {
            assert.equal(data.collection, 'longcursors');
            assert.equal(data.count, testCase.threshold + 1);
            assert.equal(data.cmd.query.notFoo.$exists, false);
            assert.include(data.stack, __filename);
            done();
          });
          collection.find({ notFoo: { $exists: false} }).limit(500).toArray(() => {});
        });
      });

      describe('when document list is smaller than threshold', function(){

        before(insertDocs(testCase.threshold - 1));

        it('should not emit an event', function(done) {
          watcher.once('long.cursor.enumeration', () => {
            done(new Error('this should not be called'));
          });
          collection.find({}).limit(50).toArray(done);
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
          assert.throw(() => new MongoWatcher(db, { longCursorCheckInterval: interval }), /Interval must be a positive save integer, found: .*/);
        });
      });
    });

    describe('event interval', function(){
      beforeEach(done => setUpWatcher({ longCursorCheckInterval: 3 }, done));
      const docListLength = defaultThreshold + 1;
      beforeEach(insertDocs(docListLength));
      afterEach(removeEventListeners);
      afterEach(cleanCollection);

      it('should emit only the first time of each interval when retrieving a big document lists', function(done){
        const events = [];

        watcher.on('long.cursor.enumeration', (data) => {
          events.push(data);
        });

        collection.find({}).toArray((err, docs) => {
          if (err) { return done(err); }
          assert.lengthOf(docs, docListLength);
          // interval: X <- here, got 1 event ("X" means event sent)
          assert.lengthOf(events, 1);

          collection.find({}).toArray((err, docs) => {
            if (err) { return done(err); }
            assert.lengthOf(docs, docListLength);
            // interval: X- <- here, got 0 more events
            assert.lengthOf(events, 1);

            collection.find({}).toArray((err, docs) => {
              if (err) { return done(err); }
              assert.lengthOf(docs, docListLength);
              // interval: X-- <- here, got 0 more events
              assert.lengthOf(events, 1);

              collection.find({}).toArray((err, docs) => {
                if (err) { return done(err); }
                assert.lengthOf(docs, docListLength);
                // interval: X--X <- here, got 1 more event
                assert.lengthOf(events, 2);
                done();
              });
            });
          });
        });
      });
    });
  });
});
