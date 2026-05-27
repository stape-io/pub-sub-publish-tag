const JSON = require('JSON');
const getAllEventData = require('getAllEventData');
const getTimestampMillis = require('getTimestampMillis');
const getType = require('getType');
const sendHttpRequest = require('sendHttpRequest');
const encodeUriComponent = require('encodeUriComponent');
const getGoogleAuth = require('getGoogleAuth');
const toBase64 = require('toBase64');
const makeString = require('makeString');

let publishUrl =
  'https://pubsub.googleapis.com/v1/projects/' +
  enc(data.project) +
  '/topics/' +
  enc(data.topic) +
  ':publish';
let method = 'POST';
let input = data.addEventData ? getAllEventData() : {};
let attributes = {};

if (data.addTimestamp) input[data.timestampFieldName] = getTimestampMillis();
if (data.customDataList) {
  data.customDataList.forEach((d) => {
    if (data.skipNilValues) {
      const dType = getType(d.value);
      if (dType === 'undefined' || dType === 'null') return;
    }
    if (getType(d.name) === 'string' && d.name.indexOf('.') !== -1) {
      const nameParts = d.name.split('.');
      let obj = input;
      for (let i = 0; i < nameParts.length - 1; i++) {
        const part = nameParts[i];
        if (!obj[part]) {
          obj[part] = {};
        }
        obj = obj[part];
      }
      obj[nameParts[nameParts.length - 1]] = d.value;
    } else {
      input[d.name] = d.value;
    }
  });
}

if (data.attributesDataList) {
  data.attributesDataList.forEach((d) => {
    if (data.skipNilValues) {
      const dType = getType(d.value);
      if (dType === 'undefined' || dType === 'null') return;
    } else {
      attributes[d.name] = d.value;
    }
  });
}

// Only send `data` and `attributes` keys if their inputs are not empty objects
let message = {};

if (input && JSON.stringify(input) !== '{}') {
  message.data = toBase64(JSON.stringify(input));
}

if (attributes && JSON.stringify(attributes) !== '{}') {
  message.attributes = attributes;
}

input = { messages: [message] };

const auth = getGoogleAuth({
  scopes: ['https://www.googleapis.com/auth/pubsub']
});

sendHttpRequest(
  publishUrl,
  { method: method, headers: { 'Content-Type': 'application/json' }, authorization: auth },
  JSON.stringify(input)
).then(
  () => {
    data.gtmOnSuccess();
  },
  () => {
    data.gtmOnFailure();
  }
);

function enc(data) {
  if (['null', 'undefined'].indexOf(getType(data)) !== -1) data = '';
  return encodeUriComponent(makeString(data));
}
