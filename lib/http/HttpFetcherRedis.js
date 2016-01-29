/*! @license ©2013 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/** A HttpFetcher downloads documents through HTTP. */

var q = require('q'),
    request = require('request'),
    util = require('util'),
    EventEmitter = require('events'),
    redis = require('redis');

// Creates a new HttpFetcher
function HttpFetcher(maxParallel) {
  EventEmitter.call(this);
  this._queue = [];    // Queue of request execution functions
  this._active = {};   // Hash of active requests
  this._pending = 0;   // The number of currently active requests
  this._maxParallel = maxParallel || 10; // Only execute this many requests in parallel
  this._redisClient = redis.createClient('6379', '127.0.0.1');
}

util.inherits(HttpFetcher, EventEmitter);

// Returns a promise for the HTTP GET request's result
HttpFetcher.prototype.get = function (url) {
  return this.request(url, 'GET');
};

// Returns a promise for the HTTP request's result
HttpFetcher.prototype.request = function (url, methodName) {
  this.emit('request', url);
  var method = methodName || 'GET', requestId = methodName + url;
  // First check whether the request was already pending
  if (requestId in this._active)
    return this._active[requestId].result;
  // If not, prepare to make a request
  var self = this, deferred = q.defer();

  // Request execution function
  function execute() {
    // Check whether the request is pending in the meantime
    if (requestId in self._active)
      return deferred.resolve(self._active[requestId].result);

      var handler = function() {};

      self._redisClient.get(url + ',' + methodName, function (err, result) {
      	if (err || !result) {
          //console.log('CACHE MISS');
          // If not, start the request
          var headers = { 'Accept': 'application/ld+json;q=1.0'},
              settings = { url: url, headers: headers, timeout: 5000, method: method, withCredentials: false, gzip: true},
              activeRequest = request(settings, onResponse);
          // Mark the request as active
          self._active[requestId] = { request: activeRequest, result: deferred.promise };
          self._pending++;
        } else {
          console.log('CACHE HIT');
          var jResult = JSON.parse(result);
          onResponse(jResult.error, jResult.response, jResult.body, false);
        }
      });
  }

  // Response callback
  function onResponse(error, response, body, addtocache) {
    // Remove the request from the active list
    delete self._active[requestId];
    self._pending--;
    self.emit('response', url);
    // Schedule a possible pending call
    var next = self._queue.shift();
    if (next) {
      process.nextTick(next);
    }

    // Return result through the deferred
    if (error) {
      if (error.code === "ETIMEDOUT") {
        console.error("retrying: " + url);
        return deferred.resolve(self.get(url));
      } else {
        return deferred.reject(new Error(error));
      }
    }
    if (response.statusCode >= 500) {
      return deferred.reject(new Error('Request failed: ' + url));
    }
    var contentType = /^[^;]+/.exec(response.headers['content-type'] || 'text/html')[0];
    if (addtocache === false) {
    } else {
      //console.log('ADD TO CACHE');
      self._redisClient.setex(url + ',' + methodName, 21600, JSON.stringify({"error": error, "response": response, "body": body}));
    }
    // for the url, take the last redirect url that can be found if there was a redirect
    deferred.resolve({ url: response.request.uri.href, type: contentType, body: body, status: response.statusCode });
  }

  // Execute if possible, queue otherwise
  if (this._pending < this._maxParallel)
    execute();
  else
    this._queue.push(execute);

  return deferred.promise;
};

// Cancels all pending requests
HttpFetcher.prototype.cancelAll = function () {
  for (var id in this._active)
    this._active[id].request.abort();
  this._active = {};
  this._queue = [];
};

module.exports = HttpFetcher;
