'use strict';

var Devebot = require('devebot');
var Promise = Devebot.require('bluebird');
var lodash = Devebot.require('lodash');
var debugx = Devebot.require('debug')('devebot:co:elasticsearch:elasticsearchBridge');

var elasticsearch = require('elasticsearch');
var ElasticsearchHelper = require('./elasticsearch-helper.js');

var Service = function(params) {
  debugx.enabled && debugx(' + constructor start ...');

  params = params || {};

  this.client = new elasticsearch.Client(params.client);
  this.helper = new ElasticsearchHelper(params.helper);

  debugx.enabled && debugx(' - constructor has finished');
};

Service.argumentSchema = {
  "id": "elasticsearchBridge",
  "type": "object",
  "properties": {
    "client": {
      "type": "object"
    },
    "helper": {
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
