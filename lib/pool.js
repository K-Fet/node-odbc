class Pool {
  constructor(options) {
    this.index = Pool.count++;
    this.availablePool = {};
    this.usedPool = {};
    this.odbc = new odbc.ODBC();

    this.options = {
      ...options,
      odbc: this.odbc,
    };
  }

  open(connectionString) {

  }
}

Pool.count = 0;

module.exports = {
  Pool,
};

Pool.prototype.open = function (connectionString, callback) {
  var self = this
    ,
    db
  ;

  //check to see if we already have a connection for this connection string
  if (self.availablePool[connectionString] && self.availablePool[connectionString].length) {
    db = self.availablePool[connectionString].shift();
    self.usedPool[connectionString].push(db);

    callback(null, db);
  } else {
    db = new Database(self.options);
    db.realClose = db.close;

    db.close = function (cb) {
      //call back early, we can do the rest of this stuff after the client thinks
      //that the connection is closed.
      cb(null);


      //close the connection for real
      //this will kill any temp tables or anything that might be a security issue.
      db.realClose(function () {
        //remove this db from the usedPool
        self.usedPool[connectionString].splice(self.usedPool[connectionString].indexOf(db), 1);

        //re-open the connection using the connection string
        db.open(connectionString, function (error) {
          if (error) {
            console.error(error);
            return;
          }

          //add this clean connection to the connection pool
          self.availablePool[connectionString] = self.availablePool[connectionString] || [];
          self.availablePool[connectionString].push(db);
          exports.debug && console.dir(self);
        });
      });
    };

    db.open(connectionString, function (error) {
      exports.debug && console.log('odbc.js : pool[%s] : pool.db.open callback()', self.index);

      self.usedPool[connectionString] = self.usedPool[connectionString] || [];
      self.usedPool[connectionString].push(db);

      callback(error, db);
    });
  }
};

Pool.prototype.close = function (callback) {
  var self = this
    ,
    required = 0
    ,
    received = 0
    ,
    connections
    ,
    key
    ,
    x
  ;

  exports.debug && console.log('odbc.js : pool[%s] : pool.close()', self.index);
  //we set a timeout because a previous db.close() may
  //have caused the a behind the scenes db.open() to prepare
  //a new connection
  setTimeout(function () {
    //merge the available pool and the usedPool
    var pools = {};

    for (key in self.availablePool) {
      pools[key] = (pools[key] || []).concat(self.availablePool[key]);
    }

    for (key in self.usedPool) {
      pools[key] = (pools[key] || []).concat(self.usedPool[key]);
    }

    exports.debug && console.log('odbc.js : pool[%s] : pool.close() - setTimeout() callback', self.index);
    exports.debug && console.dir(pools);

    if (Object.keys(pools).length == 0) {
      return callback();
    }

    for (key in pools) {
      connections = pools[key];
      required += connections.length;

      exports.debug && console.log('odbc.js : pool[%s] : pool.close() - processing pools %s - connections: %s', self.index, key, connections.length);

      for (x = 0; x < connections.length; x++) {
        (function (x) {
          //call the realClose method to avoid
          //automatically re-opening the connection
          exports.debug && console.log('odbc.js : pool[%s] : pool.close() - calling realClose() for connection #%s', self.index, x);

          connections[x].realClose(function () {
            exports.debug && console.log('odbc.js : pool[%s] : pool.close() - realClose() callback for connection #%s', self.index, x);
            received += 1;

            if (received === required) {
              callback();

              //prevent mem leaks
              self = null;
              connections = null;
              required = null;
              received = null;
              key = null;

              return;
            }
          });
        })(x);
      }
    }
  }, 2000);
};
