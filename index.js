'use strict';

const EventEmitter = require('events').EventEmitter;

const defaults = {
  longCursorThreshold: 100,
  largeInsertThreshold: 1024 * 30,
  largeFetchThreshold: 1024 * 30
};

const sizeOf = require('object-sizeof');

function getStackTrace(){
  return new Error().stack.split('\n').reduce((result, current) => {
    if (current.match(/mongodb\-watcher\/index/) ||
        current.match(/^Error$/)) {
      return result;
    }
    return result + current + '\n';
  }, '');
}

class MongoWatcher extends EventEmitter {

  constructor(db, params) {
    super();

    this._params = Object.assign({}, defaults, params);

    const self = this;

    function checkDocumentFetch(collection, stack, doc) {
      if (!doc) { return; }
      const size = sizeOf(doc);
      if (size > self._params.largeFetchThreshold) {
        self.emit('large.document.fetch', {
          collection, size, stack,
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
            const stack = getStackTrace();
            return next((err, doc) => {
              if (err) { return callback(err); }
              checkDocumentFetch(collectionName, stack, doc);
              callback(null, doc);
            });
          };
        })(newCursor.next.bind(newCursor));

        newCursor.toArray = (function(toArray) {
          return function(callback) {
            const stack = getStackTrace();

            return toArray((err, documents) => {
              if (err) { return callback(err); }
              if (documents && documents.length > self._params.longCursorThreshold) {
                self.emit('long.cursor.enumeration', {
                  collection: collectionName,
                  count:      documents.length,
                  cmd:        newCursor.cmd,
                  stack
                });
              }
              if (documents) {
                documents.forEach(doc => {
                  checkDocumentFetch(collectionName, stack, doc);
                });
              }
              return callback(null, documents);
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
        self.emit('large.document.insert', {
          size,
          collection: collectionInstance.collectionName,
          stack: getStackTrace(),
          documentId: d._id
        });
      }

      collectionInstance.insertMany = (function(insertMany) {
        return function(documents) {
          const insertResult = insertMany.apply(collectionInstance, arguments);
          documents.forEach(checkLargeDocInsert);
          return insertResult;
        };
      })(collectionInstance.insertMany);

      collectionInstance.save = (function(save) {
        return function(document) {
          const saveResult = save.apply(collectionInstance, arguments);
          checkLargeDocInsert(document);
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
