

class Database {
  constructor(options) {
    this.odbc = options.odbc || new odbc.ODBC();
    this.odbc.domain = process.domain;
    this.queue = new SimpleQueue();
    this.fetchMode = options.fetchMode;
    this.connected = false;
    this.connectTimeout = options.connectTimeout;
    this.loginTimeout = options.loginTimeout;
  }

  open(connectionString) {

  }
}

function Database(options) {
  var self = this;

  options = options || {};

  if (odbc.loadODBCLibrary) {
    if (!options.library && !module.exports.library) {
      throw new Error('You must specify a library when complied with dynodbc, '
        + 'otherwise this jams will segfault.');
    }

    if (!odbc.loadODBCLibrary(options.library || module.exports.library)) {
      throw new Error('Could not load library. You may need to specify full '
        + 'path.');
    }
  }

  self.odbc = (options.odbc) ? options.odbc : new odbc.ODBC();
  self.odbc.domain = process.domain;
  self.queue = new SimpleQueue();
  self.fetchMode = options.fetchMode || null;
  self.connected = false;
  self.connectTimeout = (options.hasOwnProperty('connectTimeout'))
    ? options.connectTimeout
    : null
  ;
  self.loginTimeout = (options.hasOwnProperty('loginTimeout'))
    ? options.loginTimeout
    : null
  ;
}

//Expose constants
Object.keys(odbc.ODBC).forEach(function (key) {
  if (typeof odbc.ODBC[key] !== 'function') {
    //On the database prototype
    Database.prototype[key] = odbc.ODBC[key];

    //On the exports
    module.exports[key] = odbc.ODBC[key];
  }
});

Database.prototype.open = function (connectionString, cb) {
  var self = this;

  if (typeof (connectionString) == 'object') {
    var obj = connectionString;
    connectionString = '';

    Object.keys(obj).forEach(function (key) {
      connectionString += key + '=' + obj[key] + ';';
    });
  }

  self.odbc.createConnection(function (err, conn) {
    if (err) return cb(err);

    self.conn = conn;
    self.conn.domain = process.domain;

    if (self.connectTimeout || self.connectTimeout === 0) {
      self.conn.connectTimeout = self.connectTimeout;
    }

    if (self.loginTimeout || self.loginTimeout === 0) {
      self.conn.loginTimeout = self.loginTimeout;
    }

    self.conn.open(connectionString, function (err, result) {
      if (err) return cb(err);

      self.connected = true;

      return cb(err, result);
    });
  });
};

Database.prototype.openSync = function (connectionString) {
  var self = this;

  self.conn = self.odbc.createConnectionSync();

  if (self.connectTimeout || self.connectTimeout === 0) {
    self.conn.connectTimeout = self.connectTimeout;
  }

  if (self.loginTimeout || self.loginTimeout === 0) {
    self.conn.loginTimeout = self.loginTimeout;
  }

  if (typeof (connectionString) == 'object') {
    var obj = connectionString;
    connectionString = '';

    Object.keys(obj).forEach(function (key) {
      connectionString += key + '=' + obj[key] + ';';
    });
  }

  var result = self.conn.openSync(connectionString);

  if (result) {
    self.connected = true;
  }

  return result;
};

Database.prototype.close = function (cb) {
  var self = this;

  self.queue.push(function (next) {
    //check to see if conn still exists (it's deleted when closed)
    if (!self.conn) {
      if (cb) cb(null);
      return next();
    }

    self.conn.close(function (err) {
      self.connected = false;
      delete self.conn;

      if (cb) cb(err);
      return next();
    });
  });
};

Database.prototype.closeSync = function () {
  var self = this;

  var result = self.conn.closeSync();

  self.connected = false;
  delete self.conn;

  return result;
};

Database.prototype.query = function (sql, params, cb) {
  var self = this;

  if (typeof (params) == 'function') {
    cb = params;
    params = null;
  }

  if (!self.connected) {
    return cb({ message: 'Connection not open.' }, [], false);
  }

  self.queue.push(function (next) {
    function cbQuery(initialErr, result) {
      fetchMore();

      function fetchMore() {
        if (self.fetchMode) {
          result.fetchMode = self.fetchMode;
        }

        result.fetchAll(function (err, data) {
          var moreResults,
            moreResultsError = null;

          try {
            moreResults = result.moreResultsSync();
          } catch (e) {
            moreResultsError = e;
            //force to check for more results
            moreResults = true;
          }

          //close the result before calling back
          //if there are not more result sets
          if (!moreResults) {
            result.closeSync();
          }

          cb(err || initialErr, data, moreResults);
          initialErr = null;

          while (moreResultsError) {
            try {
              moreResults = result.moreResultsSync();
              cb(moreResultsError, [], moreResults); // No errors left - still need to report the
                                                     // last one, though
              moreResultsError = null;
            } catch (e) {
              cb(moreResultsError, [], moreResults);
              moreResultsError = e;
            }
          }

          if (moreResults) {
            return fetchMore();
          } else {
            return next();
          }
        });
      }
    }

    if (params) {
      console.log('db::conn::query() [w/ params]');
      self.conn.query(sql, params, cbQuery);
    } else {
      console.log('db::conn::query() [no params]');
      self.conn.query(sql, cbQuery);
    }
  });
};

Database.prototype.queryResult = function (sql, params, cb) {
  var self = this;

  if (typeof (params) == 'function') {
    cb = params;
    params = null;
  }

  if (!self.connected) {
    return cb({ message: 'Connection not open.' }, null);
  }

  self.queue.push(function (next) {
    //ODBCConnection.query() is the fastest-path querying mechanism.
    if (params) {
      self.conn.query(sql, params, cbQuery);
    } else {
      self.conn.query(sql, cbQuery);
    }

    function cbQuery(err, result) {
      if (err) {
        cb(err, null);

        return next();
      }

      if (self.fetchMode) {
        result.fetchMode = self.fetchMode;
      }

      cb(err, result);

      return next();
    }
  });
};

Database.prototype.queryResultSync = function (sql, params) {
  var self = this,
    result;

  if (!self.connected) {
    throw ({ message: 'Connection not open.' });
  }

  if (params) {
    result = self.conn.querySync(sql, params);
  } else {
    result = self.conn.querySync(sql);
  }

  if (self.fetchMode) {
    result.fetchMode = self.fetchMode;
  }

  return result;
};

Database.prototype.querySync = function (sql, params) {
  var self = this,
    result;

  if (!self.connected) {
    throw ({ message: 'Connection not open.' });
  }

  if (params) {
    result = self.conn.querySync(sql, params);
  } else {
    result = self.conn.querySync(sql);
  }

  if (self.fetchMode) {
    result.fetchMode = self.fetchMode;
  }

  var data = result.fetchAllSync();

  result.closeSync();

  return data;
};

Database.prototype.beginTransaction = function (cb) {
  var self = this;

  self.conn.beginTransaction(cb);

  return self;
};

Database.prototype.endTransaction = function (rollback, cb) {
  var self = this;

  self.conn.endTransaction(rollback, cb);

  return self;
};

Database.prototype.commitTransaction = function (cb) {
  var self = this;

  self.conn.endTransaction(false, cb); //don't rollback

  return self;
};

Database.prototype.rollbackTransaction = function (cb) {
  var self = this;

  self.conn.endTransaction(true, cb); //rollback

  return self;
};

Database.prototype.beginTransactionSync = function () {
  var self = this;

  self.conn.beginTransactionSync();

  return self;
};

Database.prototype.endTransactionSync = function (rollback) {
  var self = this;

  self.conn.endTransactionSync(rollback);

  return self;
};

Database.prototype.commitTransactionSync = function () {
  var self = this;

  self.conn.endTransactionSync(false); //don't rollback

  return self;
};

Database.prototype.rollbackTransactionSync = function () {
  var self = this;

  self.conn.endTransactionSync(true); //rollback

  return self;
};

Database.prototype.columns = function (catalog, schema, table, column, callback) {
  var self = this;
  if (!self.queue) self.queue = [];

  callback = callback || arguments[arguments.length - 1];

  self.queue.push(function (next) {
    self.conn.columns(catalog, schema, table, column, function (err, result) {
      if (err) return callback(err, [], false);

      result.fetchAll(function (err, data) {
        result.closeSync();

        callback(err, data);

        return next();
      });
    });
  });
};

Database.prototype.tables = function (catalog, schema, table, type, callback) {
  var self = this;
  if (!self.queue) self.queue = [];

  callback = callback || arguments[arguments.length - 1];

  self.queue.push(function (next) {
    self.conn.tables(catalog, schema, table, type, function (err, result) {
      if (err) return callback(err, [], false);
      console.log('going to fetch all');
      result.fetchAll(function (err, data) {
        result.closeSync();

        callback(err, data);

        return next();
      });
    });
  });
};

Database.prototype.describe = function (obj, callback) {
  var self = this;

  if (typeof (callback) != 'function') {
    throw({
      error: '[node-odbc] Missing Arguments',
      message: 'You must specify a callback function in order for the describe method to work.',
    });

    return false;
  }

  if (typeof (obj) != 'object') {
    callback({
      error: '[node-odbc] Missing Arguments',
      message: 'You must pass an object as argument 0 if you want anything productive to happen in the describe method.',
    }, []);

    return false;
  }

  if (!obj.database) {
    callback({
      error: '[node-odbc] Missing Arguments',
      message: 'The object you passed did not contain a database property. This is required for the describe method to work.',
    }, []);

    return false;
  }

  //set some defaults if they weren't passed
  obj.schema = obj.schema || '%';
  obj.type = obj.type || 'table';

  if (obj.table && obj.column) {
    //get the column details
    self.columns(obj.database, obj.schema, obj.table, obj.column, callback);
  } else if (obj.table) {
    //get the columns in the table
    self.columns(obj.database, obj.schema, obj.table, '%', callback);
  } else {
    //get the tables in the database
    self.tables(obj.database, obj.schema, null, obj.type || 'table', callback);
  }
};

Database.prototype.prepare = function (sql, cb) {
  var self = this;

  self.conn.createStatement(function (err, stmt) {
    if (err) return cb(err);

    stmt.queue = new SimpleQueue();

    stmt.prepare(sql, function (err) {
      if (err) return cb(err);

      return cb(null, stmt);
    });
  });
};

Database.prototype.prepareSync = function (sql, cb) {
  var self = this;

  var stmt = self.conn.createStatementSync();

  stmt.queue = new SimpleQueue();

  stmt.prepareSync(sql);

  return stmt;
};
