'use strict';

const EventEmitter = require('events').EventEmitter;

const defaults = {
  longCursorThreshold: 100,
  longInsertThreshold: 100
};

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

            //force a count if someone is watching this event
            if (self.listenerCount('long cursor')) {
              return toArray((err, documents) => {
                if (err) { return callback(err); }
                if (documents && documents.length > self._params.longCursorThreshold) {
                  self.emit('long cursor', {
                    collection: newCursor.namespace.collection,
                    documents: documents,
                    stack: stack.split('\n').slice(2).join('\n')
                  });
                }
                return callback(null, documents);
              });
            }

            return toArray.apply(newCursor, arguments);
          };
        })(newCursor.toArray.bind(newCursor));

        return newCursor;
      };
    })(db.s.topology.cursor);

    db.collection = (function(collection){
      return function () {
        const collectionInstance = collection.apply(db, arguments);
        collectionInstance.insertMany = (function(insertMany) {
          return function(documents) {
            const insertResult = insertMany.apply(collectionInstance, arguments);
            if(documents.length > self._params.longInsertThreshold) {
              self.emit('long insert', {
                collection: collectionInstance.collectionName,
                documents,
                stack: new Error().stack.split('\n').slice(2).join('\n')
              });
            }
            return insertResult;
          };
        })(collectionInstance.insertMany);

        return collectionInstance;
      };
    })(db.collection);

  }

}


module.exports = MongoWatcher;
