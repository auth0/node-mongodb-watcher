Monitor your mongodb connection for bad queries in the client-side.

## Installation

```
npm i mongodb-watcher
```

## Usage

```javascript
//connect to mongodb
const watcher = new MongoWatcher(db, {
  longCursorThreshold:  100,
  largeInsertThreshold: 1024 * 30,
  largeFetchThreshold:  1024 * 30
});

watcher.on('long.cursor.enumeration', (data) => {
  logger.error(`Detected bad query over ${data.collection} returning ${data.count} documents. \n ${data.stack}`);
});

db.collection('apples').find().toArray((apples) => {
  res.json(apples);
});
```

## Events

### `long.cursor.enumeration`

 This event is emitted when a cursor is enumerated (.toArray) returning an array of documents with more than `longCursorThreshold` (defaults 100).

 The data of the event contains:

- `collection`: the name of the collection.
- `count`: the number of documents returned or inserted.
- `stack`: an stack trace of to identify where the call was made.
- `cmd`: it contains the `query`, `limit`, `skip`, `readPreference`, `slaveOk`, etc.

### `large.document.insert`

This event is emitted when calling `.insert([])` or `.save` with a document of size greater than `bigInsertThreshold` (defaults 30k).

- `collection`: the name of the collection.
- `stack`: an stack trace of to identify where the call was made.
- `documentId`: just for `large.document.insert`, it contains the `document._id` value.
- `size`: just for `large.document.insert`, it contains the doc size.

### `large.document.fetch`

This event is emitted when retrieving a big document from the database.

- `collection`: the name of the collection.
- `stack`: an stack trace of to identify where the call was made.
- `documentId`: just for `large.document.insert`, it contains the `document._id` value.
- `size`: it contains the document size.

**More events are welcome.**

## License

MIT 2016 . JOSE F. ROMANIELLO
