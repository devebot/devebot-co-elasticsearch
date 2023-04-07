'use strict';

const elasticsearch = require('elasticsearch');
const elasticsearchV2 = require('@elastic/elasticsearch');
const ElasticsearchHelper = require('./helper');

function Service(params = {}) {
  this.client = new elasticsearch.Client(params.client);
  this.helper = new ElasticsearchHelper(params.helper);

  this.clientV2 = new elasticsearchV2.Client(params.clientV2);
};

Service.argumentSchema = {
  "id": "elasticsearchBridge",
  "type": "object",
  "properties": {
    "client": {
      "type": "object"
    },
    "clientV2": {
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
