const utils = require('../utils');
const uuid = require('uuid/v1');
const Converter = require('aws-sdk').DynamoDB.Converter;
const EVENT_DEFAULT_REGION = 'us-east-1';
const EVENT_SOURCE = 'aws:dynamodb';
const EVENT_VERSION = '1.1';
const STREAM_VIEW_TYPE = 'NEW_AND_OLD_IMAGES'; //todo: support other types: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_StreamSpecification.html#DDB-Type-StreamSpecification-StreamViewType
const EVENT_DEFAULT_TABLE = 'some-table';
const EVENT_DEFAULT_STREAM_QUALIFIER = (new Date()).toISOString();
const EVENT_OPERATION = Object.freeze({
  INSERT: 'INSERT',
  MODIFY: 'MODIFY',
  REMOVE: 'REMOVE'
});

class DynamoDBEventGenerator {
  constructor(opts) {
    this.options = opts;
  }

  _getInsertImage(content) {
    return {
      NewImage: content
    };
  }

  _getModifyImage(content, options = this.options) {
    const NewImage = content;
    const OldImage = Object.assign(options.modifications || {}, content);
    return {
      NewImage,
      OldImage
    };
  }

  _getRemoveImage(content) {
    return {
      OldImage: content
    };
  }

  _getImageObject(data, options = this.options) {
    const content = Converter.marshall(data);
    switch (options.operation) {
      case EVENT_OPERATION.INSERT:
        return this._getInsertImage(content);
      case EVENT_OPERATION.MODIFY:
        return this._getModifyImage(content);
      case EVENT_OPERATION.REMOVE:
        return this._getRemoveImage(content);
      default:
        return this._getInsertImage(content);
    }
  }

  _getKeys(record, metadata, options = this.options) {
    let keyValues = {};
    let keyKeys = metadata.keys || options.keys;
    if (keyKeys) {
      const keys = Array.isArray(keyKeys) ? keyKeys : [keyKeys];
      keyValues = Object.assign(...keys.map(key => ({
        [key]: record[key]
      })));
    } else {
      const useIdAsKey = options.useIdAsKey === undefined ? true : options.useIdAsKey;
      if (useIdAsKey && record.id) {
        keyValues = {
          id: record.id
        }
      }
    }
    return {
      Keys: Converter.marshall(keyValues)
    };
  }

  _getApproximateCreationDateTime() {
    return Math.floor(Date.now() / 1000);
  }

  _getSequenceNumber() {
    const size = utils.randomInt(21, 40);
    return [...Array(size)].map(() => utils.randomInt(0, 9)).join('');
  }

  _getSizeBytes(record) {
    return Buffer.byteLength(JSON.stringify(record), 'utf-8')
  }

  _getEventSourceArn(options = this.options) {
    return `arn:${EVENT_SOURCE}:${options.region || EVENT_DEFAULT_REGION}:${this._getAccountId(options)}:table/${options.table || EVENT_DEFAULT_TABLE}/stream/${options.streamQualifier || EVENT_DEFAULT_STREAM_QUALIFIER}`;
  }

  _getAccountId(options = this.options) {
    return options.accountId || utils.randomInt(10 ** 12 - 1, 10 ** 11);
  }

  _getDataRecords(data) {
    const records = Array.isArray(data) ? data : [data];
    return records.map(item => {
      const {
        $,
        ...record
      } = item;
      return {
        metadata: $ || {},
        record
      }
    })
  }

  _wrapRecords(data, options = this.options) {
    const records = this._getDataRecords(data);
    return {
      Records: records.map(({
        record,
        metadata
      }) => {
        const Keys = this._getKeys(record, metadata, options);
        return Object.assign({
          eventID: uuid().replace(/\-/g, ''),
          eventName: metadata.operation || options.operation || EVENT_OPERATION.INSERT,
          eventVersion: EVENT_VERSION,
          eventSource: EVENT_SOURCE,
          awsRegion: options.region || EVENT_DEFAULT_REGION,
          dynamodb: Object.assign({
            ApproximateCreationDateTime: this._getApproximateCreationDateTime(),
            SequenceNumber: this._getSequenceNumber(),
            SizeBytes: this._getSizeBytes(record),
            StreamViewType: STREAM_VIEW_TYPE,
          }, Keys, this._getImageObject(record, options)),
          eventSourceARN: this._getEventSourceArn(options)
        });
      })
    }
  }

  generate(data, opts = {}) {
    const options = Object.assign(opts, this.options);
    return this._wrapRecords(data, options);
  }

  static generate(data, opts = {}) {
    return this.with(opts).generate(data);
  }

  static with(opts = {}) {
    return new DynamoDBEventGenerator(opts);
  }
}

module.exports = DynamoDBEventGenerator;
