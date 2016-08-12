'use strict';

var Writable = require('stream').Writable;
var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var ConsumerStream = require(libPath + 'consumerStream');
var Producer = require(libPath + 'producer');
var Client = require(libPath + 'client');
var uuid = require('node-uuid');

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

function createMessages(numberOfMessages, prefix) {
  prefix = prefix || 'message';
  var messages = [];
  for (var i = 0; i < numberOfMessages; i++) {
    messages.push(prefix + ':' + i)
  }
  return messages;
};

describe('ConsumerStream', function () {
  before(function (done) {
    client = createClient();
    producer = new Producer(client);
    producer.on('ready', function () {
      producer.createTopics([
        STREAM_TOPIC,
        STREAM_TOPIC_2
      ], false, function (err, created) {
        if (err) return done(err);

        function useNewTopics() {
          producer.send([{
            topic: STREAM_TOPIC,
            messages: 'hello kafka'
          }, {
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

  describe('Streaming', function () {
    it('should stream messages to connected sink', function (done) {
      var topics = [{
        topic: STREAM_TOPIC
      }];
      var options = {
        autoCommit: false,
        groupId: '_stream_groupId_1_test'
      };
      var stream = new ConsumerStream(client, topics, options);
      stream.on('error', noop);

      var sink = new Writable({
        objectMode: true,
        write: function (message, encoding, callback) {
          done();
          callback();
        }
      });

      stream.pipe(sink);
    });

    it('should pause consumer when downstream buffer full', function (done) {
      var topics = [{
        topic: STREAM_TOPIC_2
      }];
      var options = {
        autoCommit: false,
        groupId: '_stream_groupId_4_test'
      };
      var stream = new ConsumerStream(client, topics, options);
      stream.on('error', noop);

      var pauseConsumer = stream.consumer.pause.bind(stream.consumer);
      stream.consumer.pause = function () {
        done();
        pauseConsumer();
      }

      var sink = new Writable({
        highWaterMark: 1,
        objectMode: true,
        write: function (message, encoding, callback) {}
      });

      stream.pipe(sink);
    });
  });



  describe('events', function () {
    it('should emit error when topic not exists', function (done) {
      var topics = [{
        topic: DOES_NOT_EXIST
      }];
      var options = {
        autoCommit: false,
        groupId: '_stream_groupId_2_test'
      };
      var stream = new ConsumerStream(client, topics, options);
      stream.once('error', function () {
        stream.on('error', noop);
        done();
      });
    });
  });

});