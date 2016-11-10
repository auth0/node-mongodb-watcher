'use strict';

const EventEmitter = require('events').EventEmitter;

const defaults = {
  longCursorThreshold: 100,
  longInsertThreshold: 100,
  bigInsertThreshold:  1024 * 30
};

const sizeOf = require('object-sizeof');

class MongoWatcher extends EventEmitter {

  constructor(db, params) {
    super();

    this._params = Object.assign({}, defaults, params);

    const self = this;

    db.s.topology.cursor = (function(createCursor) {
      return function () {
        const newCursor = createCursor.apply(this, arguments);

        newCursor.toArray = (function(toArray) {
          return function(callback) {
            const stack = new Error().stack;

            return toArray((err, documents) => {
              if (err) { return callback(err); }
              if (documents && documents.length > self._params.longCursorThreshold) {
                self.emit('long cursor', {
                  collection: newCursor.namespace.collection,
                  count:      documents.length,
                  cmd:        newCursor.cmd,
                  stack:      stack.split('\n').slice(2).join('\n')
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

      function checkDocumentSize(d) {
        const size = sizeOf(d);
        if (size < self._params.bigInsertThreshold) {
          return;
        }
        self.emit('big insert', {
          collection: collectionInstance.collectionName,
          size: size,
          stack: new Error().stack.split('\n').slice(2).join('\n'),
          documentId: d._id
        });
      }

      collectionInstance.insertMany = (function(insertMany) {
        return function(documents) {
          const insertResult = insertMany.apply(collectionInstance, arguments);
          if(documents.length > self._params.longInsertThreshold) {
            self.emit('long insert', {
              collection: collectionInstance.collectionName,
              count: documents.length,
              stack: new Error().stack.split('\n').slice(2).join('\n')
            });
          }
          documents.forEach(checkDocumentSize);
          return insertResult;
        };
      })(collectionInstance.insertMany);

      collectionInstance.save = (function(save) {
        return function(document) {
          const saveResult = save.apply(collectionInstance, arguments);
          checkDocumentSize(document);
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
