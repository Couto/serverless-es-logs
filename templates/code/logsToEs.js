const zlib = require('zlib');
const utils = require('util');
const request = require('./request');
const transform = require('./transform');

const gunZip = utils.promisify(zlib.gunzip);
const toUTF8 = data => data.toString('utf8');
const toJSON = data => JSON.parse(data);
const ifTruthy = fn => data => data && fn(data);

exports.handler = async (input, context) =>
  gunZip(new Buffer(input.awslogs.data, 'base64'))
    .then(toUTF8)
    .then(toJSON)
    .then(transform)
    .then(ifTruthy(request))
    .then(success => {
      console.log('Response:', JSON.stringify(success));
      context.succeed('Success');
    })
    .catch(err => {
      console.log('Error:', JSON.stringify(err));
      context.fail(err);
    });
