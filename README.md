Monitor your mongodb connection for bad queries in the client-side.

## Installation

```
npm i mongodb-watcher
```

## Usage

```javascript
//connect to mongodb
const watcher = new MongoWatcher(db, {
  longCursorThreshold: 100,
  longInsertThreshold: 50
});

watcher.on('long.cursor.enumerated', (data) => {
  logger.error(`Detected bad query over ${data.collection} returning ${data.count} documents. \n ${data.stack}`);
});

db.collection('apples').find().toArray((apples) => {
  res.json(apples);
});
```

## Options


## Events

- `long.cursor.enumerated`: this event is emitted when a cursor is enumerated (.toArray) returning an array of documents with more than `longCursorThreshold` (defaults 100).
- `long insert`: this event is emitted when calling `.insert([])` with an array with more than `longInsertThreshold` (defaults 100) documents.
- `large.document.inserted`: this event is emitted when calling `.insert([])` or `.save` with a document of size greater than `bigInsertThreshold` (defaults 30k).

The data emitted in the events:

- `collection`: the name of the collection.
- `count`: the number of documents returned or inserted.
- `data.stack`: an stack trace of to identify where the call was made.
- `cmd`: just for `long.cursor.enumerated`, it contains the `query`, `limit`, `skip`, `readPreference`, `slaveOk`, etc.
- `documentId`: just for `large.document.inserted`, it contains the `document._id` value.
- `size`: just for `large.document.inserted`, it contains the doc size.

More events are welcome.

## License

MIT 2016 . JOSE F. ROMANIELLO
