'use strict';

var Writable = require('stream').Writable;
var Readable = require('stream').Readable;
var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var CommitStream = require(libPath + 'commitStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'client');
var Offset = require(libPath + 'offset');
var uuid = require('node-uuid');
var retry = require('retry');

var client, producer;

var TOPIC_POSTFIX = '_test_' + Date.now();
var STREAM_TOPIC = '_stream_1' + TOPIC_POSTFIX;
var STREAM_TOPIC_2 = '_stream_2' + TOPIC_POSTFIX;
var DOES_NOT_EXIST = 'does_not_exist' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || '';

function noop() {
  console.log(arguments);
}

function createClient() {
  var clientId = 'kafka-node-client-' + uuid.v4();
  return new Client(host, clientId, undefined, undefined, undefined);
}

function createFakeMessageStream(client, options){
  var offset = options.minOffset || 0;
  var stream = new Readable({
      objectMode:true,
      read: function(){
        if(offset <= options.maxOffset){
          this.push({
            offset: offset++,
            partition: options.partition || 0,
            topic: options.topic,
            value: options.value || 'hello kafka'
          });
        }
      }
    });
  return stream;
}

function createMessages(numberOfMessages, prefix) {
  prefix = prefix || 'message';
  var messages = [];
  for (var i = 0; i < numberOfMessages; i++) {
    messages.push(prefix + ':' + i)
  }
  return messages;
};

describe.only('CommitStream', function () {
  before(function (done) {
    client = createClient();
    producer = new Producer(client);
    producer.on('ready', function () {
      producer.createTopics([
        STREAM_TOPIC,
      ], false, function (err, created) {
        if (err) return done(err);

        function useNewTopics() {
          producer.send([{
            topic: STREAM_TOPIC_2,
            messages: createMessages(20)
          }], function(){
            client.close(done);
          });
        }
        // Ensure leader selection happened
        setTimeout(useNewTopics, 1000);
      });
    });
  });

  beforeEach(function () {
    client = createClient();
  });
  afterEach(function (done) {
    client.close(done);
  });

    it('should stream messages to connected sink', function (done) {
      var topics = [{
        topic: STREAM_TOPIC
      }];

      var source = createFakeMessageStream(client, {minOffset: 0, maxOffset: 10, topic: STREAM_TOPIC});

      var commitStream = new CommitStream(client);

      var offsetClient = new Offset(client, "blah");

      var operation = retry.operation({ minTimeout: 200, maxTimeout: 2000 });
      operation.attempt(function (currentAttempt) {
        offsetClient.fetch([{topic: STREAM_TOPIC, partition: 0}], function(err, res){
          console.log('offsets:',res);
          if(operation.retry(new Error('foo'))){
            return;
          }
        });
      });

      source.pipe(commitStream);
    });
});