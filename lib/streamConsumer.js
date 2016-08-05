var util = require('util');
var Readable = require('stream').Readable;
var HighLevelConsumer = require('./highLevelConsumer');

function StreamConsumer(client, topics, consumerOptions){
  if (!(this instanceof StreamConsumer))
    return new StreamConsumer(client, topics, consumerOptions);

    var consumer = new HighLevelConsumer(client, topics, consumerOptions);
    consumer.pause();
    consumer.on('message', function (ev) {
      var shouldPause = !push(ev);
      if (shouldPause) {
        consumer.pause();
      }
    });

    function read() {
      push = this.push.bind(this);
      consumer.resume();
    }

  var options = {
    objectMode: true,
    read: read
  };

  this.close = consumer.close.bind(consumer);

  Readable.call(this, options);
}

util.inherits(StreamConsumer, Readable);

module.exports = StreamConsumer;