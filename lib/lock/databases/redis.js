var util = require('util'),
  Lock = require('../base'),
  _ = require('lodash'),
  redis = Lock.use('redis');

const { resolvify, rejectify } = require('../../helpers').async();

function Redis(options) {
  Lock.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 6379,
    prefix: 'aggregatelock',
    retry_strategy: function (/* retries, cause */) {
      return false;
    }
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  if (options.url) {
    var url = require('url').parse(options.url);
    if (url.protocol === 'redis:') {
      if (url.auth) {
        var userparts = url.auth.split(':');
        options.user = userparts[0];
        if (userparts.length === 2) {
          options.password = userparts[1];
        }
      }
      options.host = url.hostname;
      options.port = url.port;
      if (url.pathname) {
        options.db = url.pathname.replace('/', '', 1);
      }
    }
  }

  this.options = options;
}

util.inherits(Redis, Lock);

_.extend(Redis.prototype, {

  connect: function (callback) {
    var self = this;

    var options = this.options;

    this.client = redis.createClient({
			socket: {
				port: options.port || options.socket,
				host: options.host,
        reconnectStrategy: options.retry_strategy,
			},
			database: options.db,
      username: options.username,
      password: options.password,
			// legacyMode: true,
		});

    this.prefix = options.prefix;

    var calledBack = false;

    this.client.on('end', function () {
      self.disconnect();
      self.stopHeartbeat();
    });

    this.client.on('error', function (err) {
      console.log(err);

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });

    this._connect().then(() => {
      self.emit('connect');

      if (self.options.heartbeat) {
        self.startHeartbeat();
      }

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });
  },

  _connect: async function() {
		if (!this.client.isOpen)
			await this.client.connect();
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
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (redis)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.client.ping().then(() => {
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

    if (this.client && this.client.isOpen)
        this.client.quit();

    this.emit('disconnect');
    if (callback) callback(null, this);
  },

  _getNewIdAsync: async function() {
    await this._connect();
    const id = await this.client.incr('nextItemId:' + this.prefix);
    return id.toString();
  },

  getNewId: function(callback) {
    this._getNewIdAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _reserveAsync: async function(workerId, aggregateId) {
      const prefixedId = this.prefix + ':' + aggregateId;

      await this._connect();
      await this.client.rPush(prefixedId, workerId);
  },

  reserve: function(workerId, aggregateId, callback) {
    this._reserveAsync(workerId, aggregateId)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getAllAsync: async function(aggregateId) {
    const prefixedId = this.prefix + ':' + aggregateId;

    await this._connect();
    return this.client.lRange(prefixedId, 0, -1);
  },

  getAll: function(aggregateId, callback) {
    this._getAllAsync(aggregateId)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _resolveAsync: async function(aggregateId) {
    var prefixedId = this.prefix + ':' + aggregateId;

    await this._connect();
    await this.client.del(prefixedId);
  },

  resolve: function(aggregateId, callback) {
    this._resolveAsync(aggregateId)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  clear: function (callback) {
    const deletePromises = [
      this.client.del('nextItemId:' + this.options.prefix),
    ];

    this.client.keys(this.options.prefix + ':*').then((keys) => {
        for (const key of keys) {
          deletePromises.push(this.client.del(key));
        }
    }),

    this._connect()
      .then(() => {
          Promise.all(deletePromises).then(resolvify(callback))
      })
      .catch(rejectify(callback));
  }

});

module.exports = Redis;
