const JSON = require('JSON');
const getRequestHeader = require('getRequestHeader');
const getAllEventData = require('getAllEventData');
const getTimestampMillis = require('getTimestampMillis');
const logToConsole = require('logToConsole');
const getContainerVersion = require('getContainerVersion');
const getType = require('getType');
const sendHttpRequest = require('sendHttpRequest');
const encodeUriComponent = require('encodeUriComponent');
const getGoogleAuth = require('getGoogleAuth');
const toBase64 = require('toBase64');

const isLoggingEnabled = determinateIsLoggingEnabled();
const traceId = isLoggingEnabled ? getRequestHeader('trace-id') : undefined;

let publishUrl = 'https://pubsub.googleapis.com/v1/projects/'+enc(data.project)+'/topics/'+enc(data.topic)+':publish';
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



if (isLoggingEnabled) {
    logToConsole(
      JSON.stringify({
          Name: 'PubSub',
          Type: 'Request',
          TraceId: traceId,
          EventName: 'Publish',
          RequestMethod: method,
          RequestUrl: publishUrl,
          RequestBody: input,
      })
    );
}

const auth = getGoogleAuth({
  scopes: ['https://www.googleapis.com/auth/pubsub']
});

sendHttpRequest(publishUrl, {method: method, headers: { 'Content-Type': 'application/json' }, authorization: auth}, JSON.stringify(input))
  .then(() => {
      if (isLoggingEnabled) {
          logToConsole(
            JSON.stringify({
                Name: 'PubSub',
                Type: 'Response',
                TraceId: traceId,
                EventName: 'Publish',
                ResponseStatusCode: 200,
                ResponseHeaders: {},
                ResponseBody: {},
            })
          );
      }

      data.gtmOnSuccess();
  }, function () {
      if (isLoggingEnabled) {
          logToConsole(
            JSON.stringify({
                Name: 'PubSub',
                Type: 'Response',
                TraceId: traceId,
                EventName: 'Publish',
                ResponseStatusCode: 500,
                ResponseHeaders: {},
                ResponseBody: {},
            })
          );
      }

      data.gtmOnFailure();
  });

function enc(data) {
    data = data || '';
    return encodeUriComponent(data);
}

function determinateIsLoggingEnabled() {
    const containerVersion = getContainerVersion();
    const isDebug = !!(containerVersion && (containerVersion.debugMode || containerVersion.previewMode));

    if (!data.logType) {
        return isDebug;
    }

    if (data.logType === 'no') {
        return false;
    }

    if (data.logType === 'debug') {
        return isDebug;
    }

    return data.logType === 'always';
}
