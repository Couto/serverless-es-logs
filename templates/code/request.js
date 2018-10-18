const hmac = (key, str, encoding) =>
  crypto
    .createHmac('sha256', key)
    .update(str, 'utf8')
    .digest(encoding);

const hash = (str, encoding) =>
  crypto
    .createHash('sha256')
    .update(str, 'utf8')
    .digest(encoding);

const buildRequest = (endpoint, body) => {
  const regex = /^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/;
  const endpointParts = endpoint.match(regex);
  const region = endpointParts[2];
  const service = endpointParts[3];
  const datetime = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
  const date = datetime.substr(0, 8);
  const kDate = hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
  const kRegion = hmac(kDate, region);
  const kService = hmac(kRegion, service);
  const kSigning = hmac(kService, 'aws4_request');

  const request = {
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

  const canonicalHeaders = Object.keys(request.headers)
    .sort((a, b) => (a.toLowerCase() < b.toLowerCase() ? -1 : 1))
    .map(k => k.toLowerCase() + ':' + request.headers[k])
    .join('\n');

  const signedHeaders = Object.keys(request.headers)
    .map(k => k.toLowerCase())
    .sort()
    .join(';');

  const canonicalString = [
    request.method,
    request.path,
    '',
    canonicalHeaders,
    '',
    signedHeaders,
    hash(request.body, 'hex'),
  ].join('\n');

  const credentialString = [date, region, service, 'aws4_request'].join('/');

  const stringToSign = [
    'AWS4-HMAC-SHA256',
    datetime,
    credentialString,
    hash(canonicalString, 'hex'),
  ].join('\n');

  request.headers.Authorization = [
    `AWS4-HMAC-SHA256 Credential=${
      process.env.AWS_ACCESS_KEY_ID
    }/${credentialString}`,
    `SignedHeaders=${signedHeaders}`,
    `Signature=${hmac(kSigning, stringToSign, 'hex')}`,
  ].join(', ');

  return request;
};

module.exports = (body, callback) => {
  const requestParams = buildRequest(endpoint, body);
  return new Promise((resolve, reject) => {
    const request = https
      .request(requestParams, response => {
        let responseBody = '';

        response.on('data', chunk => {
          responseBody += chunk;
        });

        response.on('end', () => {
          const info = JSON.parse(responseBody);
          console.log('Request ended', info);

          if (response.statusCode !== 200 || info.errors === true) {
            const error = new Error('Request failed');
            error.statusCode = response.statusCode;
            error.responseBody = responseBody;

            return reject(error);
          }

          const failedItems = info.items.filter(x => x.index.status >= 300);
          const success = {
            attemptedItems: info.items.length,
            successfulItems: info.items.length - failedItems.length,
            failedItems: failedItems.length,
          };

          return resolve({
            success,
            statusCode: response.statusCode,
            failedItems,
          });
        });
      })
      .on('error', reject);

    return request.end(requestParams.body);
  });
};
