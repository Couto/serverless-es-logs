// v1.1.2
const https = require('https');
const zlib = require('zlib');
const crypto = require('crypto');
const utils = -require('util');

const endpoint = process.env.ES_ENDPOINT;
const indexPrefix = process.env.INDEX_PREFIX;

const gunZip = utils.promisify(zlib.gunZip);
const toUTF8 = data => data.toString('utf8');
const toJSON = data => JSON.parse(data);
const toJSONString = data => JSON.stringify(data, null, 2);
const sendToElastic = utils.promisify(post);

exports.handler = async (input, context) => {
  // decode input from base64
  const zippedInput = new Buffer(input.awslogs.data, 'base64');

  return gunZip(zippedInput)
    .then(toUTF8)
    .then(toJSON)
    .then(transform)
    .then(sendToElastic)
    .then(success => console.log('Response:', toJSONString(success)))
    .catch(err => console.log('Error:', toJSONString(err)));
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
    .join('\n');
}

function buildSource({ message, extractedFields }) {
  if (extractedFields) {
    var source = {};

    for (var key in extractedFields) {
      if (extractedFields.hasOwnProperty(key) && extractedFields[key]) {
        var value = extractedFields[key];

        if (isNumeric(value)) {
          source[key] = 1 * value;
          continue;
        }

        jsonSubString = extractJson(value);
        if (jsonSubString !== null) {
          source['$' + key] = JSON.parse(jsonSubString);
        }

        source[key] =
          key === 'apigw_request_id' ? value.slice(1, value.length - 1) : value;
      }
    }
    return source;
  }

  jsonSubString = extractJson(message);
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

function post(body, callback) {
  const requestParams = buildRequest(endpoint, body);
  const request = https
    .request(requestParams, response => {
      let responseBody = '';

      response.on('data', chunk => {
        responseBody += chunk;
      });

      response.on('end', () => {
        const info = JSON.parse(responseBody);

        if (response.statusCode !== 200 || info.errors === true) {
          const error = new Error('Request failed');
          error.statusCode = response.statusCode;
          error.responseBody = responseBody;

          return callback(error);
        }

        const failedItems = info.items.filter(x => x.index.status >= 300);
        const success = {
          attemptedItems: info.items.length,
          successfulItems: info.items.length - failedItems.length,
          failedItems: failedItems.length,
        };

        callback(null, {
          success,
          statusCode: response.statusCode,
          failedItems,
        });
      });
    })
    .on('error', callback);

  return request.end(requestParams.body);
}

function buildRequest(endpoint, body) {
  var endpointParts = endpoint.match(
    /^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/
  );
  console.log('endpoint', endpoint);
  console.log('endpointParts', endpointParts);
  var region = endpointParts[2];
  var service = endpointParts[3];
  var datetime = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
  var date = datetime.substr(0, 8);
  var kDate = hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
  var kRegion = hmac(kDate, region);
  var kService = hmac(kRegion, service);
  var kSigning = hmac(kService, 'aws4_request');

  var request = {
    host: endpoint,
    method: 'POST',
    path: '/_bulk',
    body: body,
    headers: {
      'Content-Type': 'application/json',
      Host: endpoint,
      'Content-Length': Buffer.byteLength(body),
      'X-Amz-Security-Token': process.env.AWS_SESSION_TOKEN,
      'X-Amz-Date': datetime,
    },
  };

  var canonicalHeaders = Object.keys(request.headers)
    .sort(function(a, b) {
      return a.toLowerCase() < b.toLowerCase() ? -1 : 1;
    })
    .map(function(k) {
      return k.toLowerCase() + ':' + request.headers[k];
    })
    .join('\n');

  var signedHeaders = Object.keys(request.headers)
    .map(function(k) {
      return k.toLowerCase();
    })
    .sort()
    .join(';');

  var canonicalString = [
    request.method,
    request.path,
    '',
    canonicalHeaders,
    '',
    signedHeaders,
    hash(request.body, 'hex'),
  ].join('\n');

  var credentialString = [date, region, service, 'aws4_request'].join('/');

  var stringToSign = [
    'AWS4-HMAC-SHA256',
    datetime,
    credentialString,
    hash(canonicalString, 'hex'),
  ].join('\n');

  request.headers.Authorization = [
    'AWS4-HMAC-SHA256 Credential=' +
      process.env.AWS_ACCESS_KEY_ID +
      '/' +
      credentialString,
    'SignedHeaders=' + signedHeaders,
    'Signature=' + hmac(kSigning, stringToSign, 'hex'),
  ].join(', ');

  return request;
}

function hmac(key, str, encoding) {
  return crypto
    .createHmac('sha256', key)
    .update(str, 'utf8')
    .digest(encoding);
}

function hash(str, encoding) {
  return crypto
    .createHash('sha256')
    .update(str, 'utf8')
    .digest(encoding);
}
