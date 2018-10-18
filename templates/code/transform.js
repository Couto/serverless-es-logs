
const indexPrefix = process.env.INDEX_PREFIX;

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

const tryParseJSON = data => {
  try {
    return JSON.parse(data);
  } catch (e) {
    return null;
  }
};

const formatAction = (timestamp, requestId, event) => {
  // index name format: cwl-YYYY.MM.DD
  const indexName = [
    indexPrefix + '-' + timestamp.getUTCFullYear(), // year
    ('0' + (timestamp.getUTCMonth() + 1)).slice(-2), // month
    ('0' + timestamp.getUTCDate()).slice(-2), // day
  ].join('.');

  const action = { index: {} };
  action.index._index = indexName;
  action.index._type = 'serverless-es-logs';
  action.index._id = requestId;

  return action;
};

const formatMessage = (payload, timestamp, requestId, event) => {
  const fields = tryParseJSON(event);
  const defaults = {
    '@id': requestId,
    '@timestamp': timestamp,
    '@owner': payload.owner,
    '@log_group': payload.logGroup,
    '@log_stream': payload.logStream,
  };

  return fields
    ? Object.assign(
        {
          level: fields.level || 'debug',
          message: fields.message || fields.msg || event,
          fields,
        },
        defaults
      )
    : Object.assign(
        {
          level: 'debug',
          message: event,
        },
        defaults
      );
};

module.exports = payload => {
  if (payload.messageType === 'CONTROL_MESSAGE') {
    return null;
  }

  return payload.logEvents
    .map(logEvent => {
      const [date, requestId, event] = logEvent.message.split('\t', 3);
      const timestamp = new Date(date);

      return [
        JSON.stringify(formatAction(timestamp, requestId, event)),
        JSON.stringify(formatMessage(payload, timestamp, requestId, event)),
      ].join('\n');
    })
    .join('\n')
    .concat('\n');
};
