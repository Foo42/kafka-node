var util = require('util');
var Writable = require('stream').Writable;
var Offset = require('./offset');

function CommitStream(client, consumerGroup) {
  if (!(this instanceof CommitStream))
    return new CommitStream(client);

  var offset = new Offset(client);
  var options = {
      objectMode: true,
      write: function (message, encoding, callback) {
          console.log('committing', message.topic, message.partition, message.offset);
          offset.commit(
              consumerGroup, [{
                  topic: message.topic,
                  partition: message.partition,
                  offset: message.offset
              }], console.log.bind(console, 'committed:'));
          callback();
      }
  };

  Writable.call(this, options);
}


util.inherits(CommitStream, Writable);

module.exports = CommitStream;