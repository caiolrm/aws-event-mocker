const uuid = require('uuid/v1');
const url = require('url');
const moment = require('moment');
const utils = require('../utils');

class APIGatewayEventGenerator {
  constructor(opts) {
    this.options = opts;
  }

  _getApiId() {
    return uuid().replace(/\-/g, '').substr(0, 10);
  }

  _getRequestTimeEpoch() {
    return Date.now();
  }

  _getRequestTime() {
    return moment.utc().format('DD/MMM/YYYY:HH:mm:ss +0000')
  }

  _getAccountId(options = this.options) {
    return options.accountId || utils.randomInt(10 ** 12 - 1, 10 ** 11);
  }

  _getHeaders(defaults, options = this.options) {
    return Object.assign(options.headers || {}, defaults);
  }

  _getQueryStringParameters(requestUrl) {
    if (requestUrl.searchParams) {
      let result = {};
      const params = requestUrl.searchParams;
      for (const [name, value] of params) {
        result[name] = value;
      }
      return result;
    } else {
      return null;
    }
  }

  _getPathParameters(requestUrl, resource) {
    if (requestUrl.pathname) {
      let result = {};
      const resourceVars = resource.split('/');
      const requestVars = requestUrl.pathname.split('/');
      for (const [index, resourceVar] of resourceVars.entries()) {
        if (resourceVar.startsWith('{') && resourceVar.endsWith('}')) {
          result[resourceVar.replace(/[\{\}]/g, '')] = requestVars[index]
        }
      }
      return result;
    } else {
      return null;
    }
  }

  _getResourceId() {
    return uuid().replace(/\-/g, '').substr(0, 6);
  }

  _getExtendedRequestId() {
    return Buffer.from(uuid().substr(0, 6)).toString('base64');
  }

  _getIdentity(defaults, options = this.options) {
    return Object.assign(options.identity || {}, defaults);
  }

  _getBody(data, options = this.options) {
    if (options.isBase64Encoded) {
      return Buffer.from(data).toString('base64');
    } else {
      return JSON.stringify(data);
    }
  }

  _wrapEvent(data, options = this.options) {
    const requestUrl = url.parse(options.url);
    const resource = options.resource || requestUrl.pathname;
    return {
      "resource": resource,
      "path": requestUrl.pathname,
      "httpMethod": options.method || 'GET',
      "headers": this._getHeaders({
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "cache-control": "no-cache",
        "CloudFront-Forwarded-Proto": "https",
        "CloudFront-Is-Desktop-Viewer": "true",
        "CloudFront-Is-Mobile-Viewer": "false",
        "CloudFront-Is-SmartTV-Viewer": "false",
        "CloudFront-Is-Tablet-Viewer": "false",
        "Content-Type": "application/json",
        "Host": "localhost",
        "User-Agent": "EventGenerator/0.0.1"
      }, options),
      "queryStringParameters": this._getQueryStringParameters(requestUrl),
      "pathParameters": this._getPathParameters(requestUrl, resource),
      "stageVariables": null,
      "requestContext": {
        "resourceId": this._getResourceId(),
        "authorizer": options.authorizer || {},
        "resourcePath": resource,
        "httpMethod": options.method || 'GET',
        "extendedRequestId": this._getExtendedRequestId(),
        "requestTime": this._getRequestTime(),
        "path": requestUrl.pathname,
        "accountId": this._getAccountId(),
        "protocol": "HTTP/1.1",
        "stage": options.stage || "prod",
        "requestTimeEpoch": this._getRequestTimeEpoch(),
        "requestId": uuid(),
        "identity": this._getIdentity({
          "cognitoIdentityPoolId": null,
          "accountId": null,
          "cognitoIdentityId": null,
          "caller": null,
          "sourceIp": "127.0.0.1",
          "accessKey": null,
          "cognitoAuthenticationType": null,
          "cognitoAuthenticationProvider": null,
          "userArn": null,
          "userAgent": "EventGenerator/0.0.1",
          "user": null
        }, options),
        "apiId": this._getApiId()
      },
      "body": this._getBody(data, options),
      "isBase64Encoded": !!options.isBase64Encoded
    }
  }

  generate(data, opts = {}) {
    const options = Object.assign(opts, this.options);
    return this._wrapEvent(data, options);
  }

  static generate(data, opts = {}) {
    return this.with(opts).generate(data);
  }

  static with(opts = {}) {
    return new APIGatewayEventGenerator(opts);
  }
}

module.exports = APIGatewayEventGenerator;
