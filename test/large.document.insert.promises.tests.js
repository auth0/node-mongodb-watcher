const MongoClient  = require('mongodb').MongoClient;
const MongoWatcher = require('../');
const assert       = require('chai').assert;
const sizeOf       = require('object-sizeof');

describe('large.document.insert (returning promises)', function () {
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
      collection = db.collection('biginsert');
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
  const buildObjectSmallerThan = size => ({ id: 'obj', test: { withArray: [ 'a'.repeat(size/10) ] }});

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
        largeInsertThreshold: customThreshold
      }
    },
  ].forEach(function(testCase){

    describe(testCase.description, function(){
      before(function(done){
        setUpWatcher(testCase.watcherParams, done);
      });
      after(cleanCollection);
      after(removeEventListeners);

      const insertCases = [
        {
          name: 'save',
          insertFn: (coll, doc) => coll.save(doc)
        },
        {
          name: 'insert',
          insertFn: (coll, doc) => coll.insert(doc)
        },
        {
          name: 'insertOne',
          insertFn: (coll, doc) => coll.insertOne(doc)
        },
        {
          name: 'insertMany',
          insertFn: (coll, doc) => coll.insertMany([ doc ])
        }
      ];

      const updateCases = [
        {
          name: 'update',
          insertFn: (coll, doc) => coll.save({ id: 1 }, () => coll.update({ id: 1 }, doc))
        },
        {
          name: 'updateOne',
          insertFn: (coll, doc) => coll.save({ id: 1 }, () => coll.updateOne({ id: 1 }, doc))
        },
        {
          name: 'updateMany',
          insertFn: (coll, doc) => coll.save({ id: 1 }, () => coll.updateMany({ id: 1 }, { $set: doc })),
          sizeOf: doc => sizeOf({ $set: doc })
        }
      ];

      describe('when object is bigger than threshold', function(){
        insertCases.forEach(function(insertCase){
          it(`should emit an event when inserting with ${insertCase.name}()`, function(done) {
            const badDoc = buildObjectSlightlyBiggerThan(testCase.threshold);

            watcher.once('large.document.insert', (data) => {
              assert.isOk(badDoc._id);
              assert.equal(data.collection, 'biginsert');
              assert.equal(data.size, sizeOf(badDoc));
              assert.equal(data.documentId, badDoc._id);
              assert.include(data.stack, __filename);
              done();
            });

            insertCase.insertFn(collection, badDoc);
          });
        });

        updateCases.forEach(function(updateCase){
          it(`should emit an event when updating with ${updateCase.name}()`, function(done) {
            const badDoc = buildObjectSlightlyBiggerThan(testCase.threshold);

            watcher.once('large.document.insert', (data) => {
              assert.isUndefined(badDoc._id); // the updates object usually don't have the
              assert.isUndefined(data.documentId); // the updates object usually don't have the
              assert.equal(data.collection, 'biginsert');
              assert.equal(data.size, updateCase.sizeOf ? updateCase.sizeOf(badDoc) : sizeOf(badDoc));
              assert.include(data.stack, __filename);
              done();
            });

            updateCase.insertFn(collection, badDoc);
          });
        });
      });


      describe('when object is smaller than threshold', function(){
        insertCases.concat(updateCases).forEach(function(testCase){
          it(`should not emit the event when executing ${testCase.name}()`, function() {
            const goodDoc = buildObjectSmallerThan(testCase.threshold);

            watcher.once('large.document.insert', () => {
              done(new Error('this should not be emitted the document is ' + sizeOf(goodDoc)));
            });

            return testCase.insertFn(collection, goodDoc);
          });
        });
      });
    });
  });

  describe('when a custom check interval is used', function(){

    describe('event intervals', function(){
      beforeEach(function(done){
        setUpWatcher({ largeInsertCheckInterval: 3 }, done);
      });
      afterEach(removeEventListeners);

      const buildBigDoc = () => buildObjectSlightlyBiggerThan(defaultThreshold);

      it('should emit only the first time of each interval when inserting a big document', function(){
        const events = [];

        watcher.on('large.document.insert', (data) => {
          events.push(data);
        });

        return collection.save(buildBigDoc()).then(() => {
          // interval: X <- here, got 1 event ("X" means event sent)
          assert.lengthOf(events, 1)

          return collection.insertMany([ buildBigDoc(), buildBigDoc() ]).then(() => {
            // interval: X-- <- here, got 0 events
            assert.lengthOf(events, 1)

            return collection.insertMany([ buildBigDoc(), buildBigDoc() ]).then(() => {
              // interval: X--X- <- here, got 1 event
              assert.lengthOf(events, 2)

              return collection.insert(buildBigDoc()).then(() => {
                // interval: X--X-- <- here, got 0 events
                assert.lengthOf(events, 2)

                return collection.insertOne(buildBigDoc()).then(() => {
                  // interval: X--X--X <- here, got 1 events
                  assert.lengthOf(events, 3)
                });
              });
            });
          });
        });
      });

      it('should emit only the first time of each interval when updating a big document', function(){
        const events = [];

        watcher.on('large.document.insert', (data) => {
          events.push(data);
        });

        return collection.save(buildBigDoc()).then(() => {
          // interval: X <- here, got 1 event ("X" means event sent)
          assert.lengthOf(events, 1)

          return collection.update({ id: 'obj' }, buildBigDoc()).then(() => {
            // interval: X- <- here, got 0 events
            assert.lengthOf(events, 1)

            return collection.updateOne({ id: 'obj' }, buildBigDoc()).then(() => {
              // interval: X-- <- here, got 0 events
              assert.lengthOf(events, 1)

              return collection.updateMany({ id: 'obj' }, { $set: buildBigDoc() }).then(() => {
                // interval: X--X <- here, got 1 event
                assert.lengthOf(events, 2)
              });
            });
          });
        });
      });
    });
  });
});
