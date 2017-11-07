'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debuglog = Devebot.require('debug')('devebot:co:elasticsearch:elasticsearchBridge');

var elasticsearch = require('elasticsearch');
var ElasticsearchDirect = require('./elasticsearch-direct.js');

var Service = function(params) {
  debuglog.enabled && debuglog(' + constructor start ...');

  params = params || {};

  this.client = new elasticsearch.Client(params.client);
  this.direct = new ElasticsearchDirect(params.direct);

  debuglog.enabled && debuglog(' - constructor has finished');
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
