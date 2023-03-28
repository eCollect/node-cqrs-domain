var util = require('util'),
  Bumper = require('../base'),
  _ = require('lodash'),
  mongo = Bumper.use('mongodb'),
  mongoVersion = Bumper.use('mongodb/package.json').version,
  ObjectId = mongo.ObjectId;

const { resolvify, rejectify } = require('../../helpers').async();

function Mongo(options) {
  Bumper.call(this, options);

  var defaults = {
    host: '127.0.0.1',
    port: 27017,
    dbName: 'domain',
    collectionName: 'commandbumper',
    // heartbeat: 60 * 1000
    ttl:  1000 * 60 * 60 * 1 // 1 hour
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

util.inherits(Mongo, Bumper);

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

      this.bumper = this.db.collection(options.collectionName);
      await this.bumper.createIndex({ expires: 1 }, { expireAfterSeconds: 0 });
      
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

  _addAsync: async function(key, ttl) {
    var exp = new Date(Date.now() + ttl);
    await this.bumper.insertOne({ _id: key, expires: exp }, { writeConcern: { w: 1 } });

    setTimeout(() => {
      this.bumper.deleteOne({ _id: key }, { writeConcern: { w: 1 } }).then(() => {});
    }, ttl);

    return true;
  },

  add: function(key, ttl, callback) {
    if (!callback) {
      callback = ttl;
      ttl = this.options.ttl;
    }

    this._addAsync(key, ttl)
      .then(resolvify(callback))
      .catch((err) => {
          if (err && err.message && err.message.indexOf('duplicate key') >= 0)
            return callback(null, false);

          if (err)
            return callback(err);
      });
  },

  clear: function (callback) {
    this.bumper.deleteMany({}, { writeConcern: { w: 1 } })
       .then(resolvify(callback))
       .catch(rejectify(callback));
  }
});

module.exports = Mongo;
