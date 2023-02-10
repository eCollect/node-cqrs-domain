var util = require('util'),
  Lock = require('../base'),
  _ = require('lodash'),
  mongo = Lock.use('mongodb'),
  mongoVersion = Lock.use('mongodb/package.json').version,
  ObjectId = mongo.ObjectId;

const { resolvify, rejectify } = require('../../helpers').async();

function Mongo(options) {
  Lock.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 27017,
    dbName: 'domain',
    collectionName: 'aggregatelock',
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  var defaultOpt = {
    ssl: false
  };

  options.options = options.options || {};

  defaultOpt.useNewUrlParser = true;
  defaultOpt.useUnifiedTopology = true;
  _.defaults(options.options, defaultOpt);

  this.options = options;
}

util.inherits(Mongo, Lock);

_.extend(Mongo.prototype, {

  _connectAsync: async function () {
    var options = this.options;

    var connectionUrl;

    if (options.url) {
      connectionUrl = options.url;
    } else {
      var members = options.servers
        ? options.servers
        : [{host: options.host, port: options.port}];

      var memberString = _(members).map(function(m) { return m.host + ':' + m.port; });
      var authString = options.username && options.password
        ? options.username + ':' + options.password + '@'
        : '';
      var optionsString = options.authSource
        ? '?authSource=' + options.authSource
        : '';

      connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
    }

    var client = new mongo.MongoClient(connectionUrl, options.options);
    const cl = await client.connect();
    this.db = cl.db(cl.s.options.dbName);

    if (!this.db.close)
      this.db.close = cl.close.bind(cl);

    cl.on('serverClosed', () => {
      this.emit('disconnect');
      this.stopHeartbeat();
    });

    this.lock = this.db.collection(options.collectionName);
    await this.lock.createIndex({ aggregateId: 1, date: 1 });
    
    this.emit('connect');

    if (this.options.heartbeat)
        this.startHeartbeat();
  },

  connect: function (callback) {
    this._connectAsync().then(() => {
      if (callback) callback(null, this);
    }).catch(rejectify(callback))
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    if (this.heartbeatInterval)
      return; 

    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.db.admin().ping()
        .then(() => {
            if (graceTimer) clearTimeout(graceTimer);
        }).catch((err) => {
            if (graceTimer) clearTimeout(graceTimer);

            console.error(err.stack || err);
            self.disconnect();
        });
      
    }, this.options.heartbeat);
  },

  disconnect: function (callback) {
    this.stopHeartbeat();

    if (!this.db) {
      if (callback) callback(null);
      return;
    }

    this.db.close()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  getNewId: function(callback) {
    callback(null, new ObjectId().toHexString());
  },

  reserve: function(workerId, aggregateId, callback) {
    this.lock.replaceOne({ _id: workerId }, { _id: workerId, aggregateId: aggregateId, date: new Date() }, {
        safe: true,
        upsert: true,
        writeConcern: { w: 1 }
    }).then(() => {
      if (callback) callback();
    }).catch(rejectify(callback));
  },

  getAll: function(aggregateId, callback) {
    this.lock.find({ aggregateId: aggregateId }).sort({ date: 1 }).toArray()
      .then((res) => {
        callback(null, _.map(res, function (entry) { return entry._id; }));
      })
      .catch(rejectify(callback));
  },

  resolve: function(aggregateId, callback) {
    this.lock.deleteMany({ aggregateId: aggregateId }, { writeConcern: { w: 1 } })
      .then(() => {
        if (callback) callback();
      }).catch(rejectify(callback));
  },

  clear: function (callback) {
    this.lock.deleteMany({}, { writeConcern: { w: 1 } })
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }

});

module.exports = Mongo;
