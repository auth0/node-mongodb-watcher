'use strict';

const EventEmitter = require('events').EventEmitter;

const defaults = {
  longCursorThreshold: 100,
  longCursorCheckInterval: 1,
  largeInsertThreshold: 1024 * 30,
  largeInsertCheckInterval: 1,
  largeFetchThreshold: 1024 * 30,
  largeFetchCheckInterval: 1
};

const sizeOf = require('object-sizeof');

/**
 * This is a workaround to this performance issue when accessing the stack:
 * https://bugs.chromium.org/p/v8/issues/detail?id=5962
 *
 * This will first capture the stacktrace by creating an error, the returned
 * function will return the proper formatted stack trace.
 *
 */
function captureStackTrace(){
  const error = new Error();

  return function() {
    return formatStackTrace(error.stack);
  }
}

function formatStackTrace(stackTrace){
  return stackTrace.split('\n').reduce((result, current) => {
    if (current.match(/mongodb\-watcher\/index/) ||
        current.match(/^Error$/)) {
      return result;
    }
    return result + current + '\n';
  }, '');
}

/**
 * This function creates a sequence generator-like where the returned function will result on "true" at the
 * beginning of every interval of calls, false in the rest of cases.
 * The caller should take a sample when "true" is returned
 *
 * @param {Number} interval Defines the interval for the sampling
 * For example:
 *   - 1:  Every call to "shouldRunSample" it would return true.
 *   - 2:  Every 2 calls to "shouldRunSample" it would return true for one.
 *   - 10: Every 10 calls to "shouldRunSample" it would return true for one.
 */
function createSampling(interval) {
  if (!Number.isSafeInteger(interval) || interval <= 0) {
    throw new TypeError('Interval must be a positive save integer, found: ' + interval);
  }

  var execCount = 0;

  return function shouldRunSample() {
    execCount = execCount === interval ? 1 : execCount + 1;

    return execCount === 1;
  }
}

class MongoWatcher extends EventEmitter {

  constructor(db, params) {
    super();

    this._params = Object.assign({}, defaults, params);

    const shouldCheckForLongCursor = createSampling(this._params.longCursorCheckInterval);
    const shouldCheckForLargeInsert = createSampling(this._params.largeInsertCheckInterval);
    const shouldCheckForLargeFetch = createSampling(this._params.largeFetchCheckInterval);

    const self = this;

    function checkDocumentFetch(collection, getFormattedStack, cmd, doc) {
      if (!doc) { return; }
      const size = sizeOf(doc);
      if (size > self._params.largeFetchThreshold) {
        self.emit('large.document.fetch', {
          collection, size, cmd,
          stack: getFormattedStack(),
          documentId: doc._id
        });
      }
    }

    db.s.topology.cursor = (function(createCursor) {
      return function () {
        const newCursor = createCursor.apply(this, arguments);

        const collectionName = newCursor.namespace &&
                               newCursor.namespace.collection ||
                               '';


        newCursor.next = (function(next) {
          return function(callback) {
            if (!shouldCheckForLargeFetch()) {
              return next(callback);
            }

            const getFormattedStack = captureStackTrace();
            return next((err, doc) => {
              if (err) { return callback(err); }
              checkDocumentFetch(collectionName, getFormattedStack, newCursor.cmd, doc);
              callback(null, doc);
            });
          };
        })(newCursor.next.bind(newCursor));

        newCursor.toArray = (function(toArray) {
          return function(callback) {
            const runLongCursorCheck = shouldCheckForLongCursor();
            var runLargeFetchCheck = shouldCheckForLargeFetch();
            const shouldCheckAnything = runLongCursorCheck || runLargeFetchCheck;

            if (!shouldCheckAnything) {
              return toArray(callback);
            }

            const getFormattedStack = captureStackTrace();

            const runChecks = (documents) => {
              if (documents) {
                if (runLongCursorCheck && documents.length > self._params.longCursorThreshold) {
                  self.emit('long.cursor.enumeration', {
                    collection: collectionName,
                    count:      documents.length,
                    cmd:        newCursor.cmd,
                    stack:      getFormattedStack()
                  });
                }
                documents.forEach((doc, index) => {
                  if (runLargeFetchCheck) {
                    checkDocumentFetch(collectionName, getFormattedStack, newCursor.cmd, doc);
                  }
                  if (index < documents.length - 1) {
                    runLargeFetchCheck = shouldCheckForLargeFetch();
                  }
                });
              }
            };

            return typeof callback === 'function' ?
              toArray((err, documents) => {
                if (err) { return callback(err); }
                runChecks(documents);
                return callback(null, documents);
              }) :
              toArray().then((documents) => {
                runChecks(documents);
                return documents;
              });
          };
        })(newCursor.toArray.bind(newCursor));

        return newCursor;
      };
    })(db.s.topology.cursor);

    function patchCollection(collectionInstance) {
      function checkLargeDocInsert(d) {
        const size = sizeOf(d);
        if (size < self._params.largeInsertThreshold) {
          return;
        }
        const getFormattedStack = captureStackTrace();
        self.emit('large.document.insert', {
          size,
          collection: collectionInstance.collectionName,
          stack: getFormattedStack(),
          documentId: d._id
        });
      }

      collectionInstance.insertMany = (function(insertMany) {

        return function(documents) {
          const insertResult = insertMany.apply(collectionInstance, arguments);
          documents.forEach(doc => {
            if (shouldCheckForLargeInsert()) {
              checkLargeDocInsert(doc);
            }
          });
          return insertResult;
        };
      })(collectionInstance.insertMany);

      collectionInstance.save = (function(save) {
        return function(document) {
          const saveResult = save.apply(collectionInstance, arguments);
          if (shouldCheckForLargeInsert()) {
            checkLargeDocInsert(document);
          }
          return saveResult;
        };
      })(collectionInstance.save);

      return collectionInstance;
    }

    db.collection = (function(collection){
      return function (name, options, callback) {
        if(typeof options === "function") {
          callback = options;
          options = {};
        }

        if (typeof callback === 'undefined') {
          return patchCollection(collection(name, options));
        }

        collection(name, options, (err, collectionInstance) => {
          if (err) { return callback(err); }
          if (!collectionInstance) { return callback(); }
          callback(null, patchCollection(collectionInstance));
        });
      };
    })(db.collection.bind(db));

  }

}


module.exports = MongoWatcher;
