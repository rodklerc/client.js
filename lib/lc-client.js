var Planner = require('csa').BasicCSA,
    Fetcher = require('./Fetcher'),
    QueryGraphBuilder = require('stp-csa').QueryGraphBuilder,
    TripExtractor = require('stp-csa').TripExtractor,
    UriTemplate = require('uritemplate');

// Example of tripURItemplate:
// http://belgianrail.linkedconnections.org/trips/{trip_id}{?date}
var Client = function (config, stpData, stopToTrip, tripURItemplate) {
  // Validate config
  this._config = config;
  this._stpData = stpData;
  this._stopToTrip = stopToTrip;
  this._tripURItemplate = UriTemplate.parse(tripURItemplate);
}

Client.prototype.query = function (q, cb) {
  //1. Validate query
  if (q.departureTime) {
    q.departureTime = new Date(q.departureTime);
  } else {
    throw "Date of departure not set";
  }
  if (!q.departureStop) {
    throw "Location of departure not set";
  }

  // Use transfer patterns only when departure AND arrival stop are specified in
  // the query. (Otherwise we would have to process the whole graph corresponding
  // to the cluster of the only stop specified.)
  if (q.departureStop && q.arrivalStop) {
    // Use a QueryGraphBuilder to produce the query graph. The output is then
    // processed by a TripExtractor which produces trip ids. We transform these
    // trip ids into URL's using the given URI template. This collection of
    // URL's will be the entry points of the fetcher.
    var qgb = new QueryGraphBuilder(q.departureStop, q.arrivalStop,
                                    this._stpData.graphs[this._stpData.inverseClustering[q.departureStop]],
                                    this._stpData.graphs[this._stpData.inverseClustering[q.arrivalStop]],
                                    this._stpData.inverseClustering,
                                    this._stpData.convexity,
                                    this._stpData.borderStations
                                  );
    var tripExtractor = new TripExtractor(this._stopToTrip);
    var self = this;
    // do NOT use the original entry point, it will cause all connections to
    // be scanned
    self._config.entrypoints = [];
    tripExtractor.on('data', function (tripId) {
      self._config.entrypoints.push(self._tripURItemplate.expand({trip_id: tripId, "date": q.departureTime.toISOString()}));
    });
    tripExtractor.on('end', function () {
      console.log('extracted trips');
      console.log(self._config.entrypoints);
      var fetcher = new Fetcher(self._config);
      self._queryHelper(q, cb, fetcher);
    });
    qgb.pipe(tripExtractor);
    qgb.end();
  } else {
    // Create fetcher
    var fetcher = new Fetcher(this._config);
    this._queryHelper(q, cb, fetcher);
  }
};

// contains part of the code originally in the query function (namely the part
// after the Fetcher has been constructed).
Client.prototype._queryHelper = function (q, cb, fetcher) {
  var query = q, self = this;

  //2. Use query to configure the data fetchers
  fetcher.buildConnectionsStream(q, function (connectionsStream) {
    //3. fire results using CSA.js and return the stream
    var planner = new Planner(q);
    //When a result is found, stop the stream
    planner.on("result", function () {
      fetcher.close();
    });
    cb(connectionsStream.pipe(planner), fetcher);
  });
};

if (typeof window !== "undefined") {
  window.lc = {
    Client : Client
  };
}

module.exports = Client;
module.exports.Fetcher = require('./Fetcher');
