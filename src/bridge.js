'use strict';

const elasticsearch = require('elasticsearch');
const ElasticsearchHelper = require('./helper');

function Service(params = {}) {
  this.client = new elasticsearch.Client(params.client);
  this.helper = new ElasticsearchHelper(params.helper);
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
