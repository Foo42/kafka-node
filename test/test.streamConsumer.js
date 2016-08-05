'use strict';

var Writable = require('stream').Writable;
var libPath = process.env['KAFKA_COV'] ? '../lib-cov/' : '../lib/';
var Consumer = require(libPath + 'consumer');
var StreamConsumer = require(libPath + 'streamConsumer');
var Producer = require(libPath + 'producer');
var Offset = require(libPath + 'offset');
var Client = require(libPath + 'client');
var should = require('should');
var uuid = require('node-uuid');
var TopicsNotExistError = require(libPath + 'errors').TopicsNotExistError;
var FakeClient = require('./mocks/mockClient');
var InvalidConfigError = require('../lib/errors/InvalidConfigError');

var client, stream, producer, offset;

var TOPIC_POSTFIX = '_test_' + Date.now();
var STREAM_TOPIC = '_stream_1' + TOPIC_POSTFIX;

var host = process.env['KAFKA_TEST_HOST'] || '';
function noop () { console.log(arguments); }

function createClient () {
  var clientId = 'kafka-node-client-' + uuid.v4();
  return new Client(host, clientId, undefined, undefined, undefined);
}

describe('StreamConsumer', function () {
  before(function (done) {
    client = createClient();
    producer = new Producer(client);
    offset = new Offset(client);
    producer.on('ready', function () {
      producer.createTopics([
        STREAM_TOPIC
      ], false, function (err, created) {
        if (err) return done(err);

        function useNewTopics () {
          producer.send([
            { topic: STREAM_TOPIC, messages: 'hello kafka' }
          ], done);
        }
        // Ensure leader selection happened
        setTimeout(useNewTopics, 1000);
      });
    });
  });

  after(function (done) {
    stream.close(done);
  });

  describe('events', function () {
    it.only('should emit message when get new message', function (done) {
      var topics = [ { topic: STREAM_TOPIC } ];
      var options = { autoCommit: false, groupId: '_groupId_1_test' };
      stream = new StreamConsumer(client, topics, options);

      var sink = new Writable({
          objectMode: true,
          write: function (message, encoding, callback) {
              done();
              callback();
          }
      });

      stream.pipe(sink);
    });
  });
});
