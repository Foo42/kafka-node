var util = require('util');
var Readable = require('stream').Readable;
var HighLevelConsumer = require('./highLevelConsumer');

function ConsumerStream(client, topics, consumerOptions) {
  if (!(this instanceof ConsumerStream))
    return new ConsumerStream(client, topics, consumerOptions);

  var push;

  consumerOptions.paused = true;
  var consumer = new HighLevelConsumer(client, topics, consumerOptions);
  consumer.on('message', function (ev) {
    var shouldPause = !push(ev);
    if (shouldPause && !consumer.paused) {
      consumer.pause();
    }
  });

  function read() {
    push = this.push.bind(this);
    if (consumer.paused) {
      consumer.resume();
    }
  }

  var options = {
    objectMode: true,
    read: read
  };

  this.consumer = consumer;
  this.close = consumer.close.bind(consumer);
  consumer.on('error', this.emit.bind(this, 'error'));

  Readable.call(this, options);
}

util.inherits(ConsumerStream, Readable);

module.exports = ConsumerStream;;;;;