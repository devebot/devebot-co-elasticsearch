'use strict';

var events = require('events');
var util = require('util');

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debug = Devebot.require('debug');
var debuglog = debug('devebot:co:elasticsearch:elasticsearchBridge');

var elasticsearch = require('elasticsearch');
var ElasticsearchDirect = require('./elasticsearch-direct.js');

var Service = function(params) {
  debuglog(' + constructor start ...');

  params = params || {};
  var self = this;

  self.client = new elasticsearch.Client(params.client);
  self.direct = new ElasticsearchDirect(params.direct);

  self.getServiceInfo = function() {
    return {};
  };

  self.getServiceHelp = function() {
    return {};
  };

  debuglog(' - constructor has finished');
};

Service.argumentSchema = {
  "id": "elasticsearchBridge",
  "type": "object",
  "properties": {
    "client": {
      "type": "object"
    },
    "direct": {
      "type": "object",
      "properties": {
        "tracking_code": {
          "type": "string"
        },
        "connection_options": {
          "type": "object"
        },
        "structure": {
          "type": "object"
        }
      }
    }
  }
};

module.exports = Service;
