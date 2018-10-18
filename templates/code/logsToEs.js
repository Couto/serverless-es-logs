const https = require('https');
const zlib = require('zlib');
const crypto = require('crypto');
const utils = require('util');
const request = require('./request');

const endpoint = process.env.ES_ENDPOINT;
const indexPrefix = process.env.INDEX_PREFIX;

const gunZip = utils.promisify(zlib.gunzip);
const toUTF8 = data => data.toString('utf8');
const toJSON = data => JSON.parse(data);
const toJSONString = data => JSON.stringify(data, null, 2);

const tap = fn => val => (fn(val), val);

exports.handler = async (input, context) => {
  // decode input from base64
  const zippedInput = new Buffer(input.awslogs.data, 'base64');

  return gunZip(zippedInput)
    .then(toUTF8)
    .then(toJSON)
    .then(transform)
    .then(tap(console.log.bind(console, 'After transform')))
    .then(request)
    .then(success => {
      console.log('Response:', toJSONString(success));
      context.succeed('Success');
    })
    .catch(err => {
      console.log('Error:', toJSONString(err));
      context.fail(err);
    });
};

// once decoded, the CloudWatch invocation event looks like this:
// {
//     "messageType": "DATA_MESSAGE",
//     "owner": "374852340823",
//     "logGroup": "/aws/lambda/big-mouth-dev-get-index",
//     "logStream": "2018/03/20/[$LATEST]ef2392ba281140eab63195d867c72f53",
//     "subscriptionFilters": [
//         "LambdaStream_logging-demo-dev-ship-logs"
//     ],
//     "logEvents": [
//         {
//             "id": "33930704242294971955536170665249597930924355657009987584",
//             "timestamp": 1521505399942,
//             "message": "START RequestId: e45ea8a8-2bd4-11e8-b067-ef0ab9604ab5 Version: $LATEST\n"
//         },
//         {
//             "id": "33930707631718332444609990261529037068331985646882193408",
//             "timestamp": 1521505551929,
//             "message": "2018-03-20T00:25:51.929Z\t3ee1bd8c-2bd5-11e8-a207-1da46aa487c9\t{ \"message\": \"found restaurants\" }\n",
//             "extractedFields": {
//                 "event": "{ \"message\": \"found restaurants\" }\n",
//                 "request_id": "3ee1bd8c-2bd5-11e8-a207-1da46aa487c9",
//                 "timestamp": "2018-03-20T00:25:51.929Z"
//             }
//         }
//     ]
// }

function transform(payload) {
  if (payload.messageType === 'CONTROL_MESSAGE') {
    return null;
  }

  return payload.logEvents
    .map(logEvent => {
      const timestamp = new Date(1 * logEvent.timestamp);

      // index name format: cwl-YYYY.MM.DD
      const indexName = [
        indexPrefix + '-' + timestamp.getUTCFullYear(), // year
        ('0' + (timestamp.getUTCMonth() + 1)).slice(-2), // month
        ('0' + timestamp.getUTCDate()).slice(-2), // day
      ].join('.');

      const source = buildSource(logEvent);
      source['@id'] = logEvent.id;
      source['@timestamp'] = new Date(1 * logEvent.timestamp).toISOString();
      source['@message'] = logEvent.message;
      source['@owner'] = payload.owner;
      source['@log_group'] = payload.logGroup;
      source['@log_stream'] = payload.logStream;

      const action = { index: {} };
      action.index._index = indexName;
      action.index._type = 'serverless-es-logs';
      action.index._id = logEvent.id;

      return [JSON.stringify(action), JSON.stringify(source)].join('\n');
    })
    .concat('\n')
    .join('\n');
}

function buildSource({ message, extractedFields }) {
  if (extractedFields) {
    return Object.keys(extractedFields).reduce((acc, key) => {
      const value = extractedFields[key];

      if (key === 'apigw_request_id') {
        return Object.assign({}, acc, {
          [key]: value.slice(1, value.length - 1),
        });
      }

      if (isNumeric(value)) {
        return Object.assign({}, acc, {
          [key]: 1 * value,
        });
      }

      const jsonValue = extractJson(value);
      if (jsonValue !== null) {
        return Object.assign({}, acc, {
          [key]: toJSON(jsonValue),
        });
      }

      return Object.assign({}, acc, { [key]: value });
    }, {});
  }

  const jsonSubString = extractJson(message);
  if (jsonSubString !== null) {
    return JSON.parse(jsonSubString);
  }

  return {};
}

function extractJson(message) {
  var jsonStart = message.indexOf('{');
  if (jsonStart < 0) return null;
  var jsonSubString = message.substring(jsonStart);
  return isValidJson(jsonSubString) ? jsonSubString : null;
}

function isValidJson(message) {
  try {
    JSON.parse(message);
  } catch (e) {
    return false;
  }
  return true;
}

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}


