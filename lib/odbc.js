/*
  Copyright (c) 2013, Dan VerWeire <dverweire@gmail.com>
  Copyright (c) 2010, Lee Smith <notwink@gmail.com>
  Permission to use, copy, modify, and/or distribute this software for any
  purpose with or without fee is hereby granted, provided that the above
  copyright notice and this permission notice appear in all copies.
  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

const odbc = require('bindings')('odbc_bindings');
const SimpleQueue = require('./simple-queue');
const util = require('util');

module.exports = function (options) {
  return new Database(options);
};

module.exports.debug = false;

module.exports.Database = Database;
module.exports.ODBC = odbc.ODBC;
module.exports.ODBCConnection = odbc.ODBCConnection;
module.exports.ODBCStatement = odbc.ODBCStatement;
module.exports.ODBCResult = odbc.ODBCResult;
module.exports.loadODBCLibrary = odbc.loadODBCLibrary;

// These constants are accessed like odbc.FETCH_ARRAY
// Once exported from here, they are no longer constant and can be changed!
module.exports.FETCH_ARRAY = odbc.FETCH_ARRAY;
module.exports.SQL_USER_NAME = odbc.SQL_USER_NAME;

module.exports.open = function (connectionString, options, cb) {
  var db;

  if (typeof options === 'function') {
    cb = options;
    options = null;
  }

  db = new Database(options);

  db.open(connectionString, function (err) {
    cb(err, db);
  });
};


//Proxy all of the asynchronous functions so that they are queued
odbc.ODBCStatement.prototype._execute = odbc.ODBCStatement.prototype.execute;
odbc.ODBCStatement.prototype._executeDirect = odbc.ODBCStatement.prototype.executeDirect;
odbc.ODBCStatement.prototype._executeNonQuery = odbc.ODBCStatement.prototype.executeNonQuery;
odbc.ODBCStatement.prototype._prepare = odbc.ODBCStatement.prototype.prepare;
odbc.ODBCStatement.prototype._bind = odbc.ODBCStatement.prototype.bind;

odbc.ODBCStatement.prototype.execute = function (params, cb) {
  var self = this;

  self.queue = self.queue || new SimpleQueue();

  if (!cb) {
    cb = params;
    params = null;
  }

  self.queue.push(function (next) {
    //If params were passed to this function, then bind them and
    //then execute.
    if (params) {
      self._bind(params, function (err) {
        if (err) {
          return cb(err);
        }

        self._execute(function (err, result) {
          cb(err, result);

          return next();
        });
      });
    }
    //Otherwise execute and pop the next bind call
    else {
      self._execute(function (err, result) {
        cb(err, result);

        //NOTE: We only execute the next queued bind call after
        // we have called execute() or executeNonQuery(). This ensures
        // that we don't call a bind() a bunch of times without ever
        // actually executing that bind. Not
        self.bindQueue && self.bindQueue.next();

        return next();
      });
    }
  });
};

odbc.ODBCStatement.prototype.executeDirect = function (sql, cb) {
  var self = this;

  self.queue = self.queue || new SimpleQueue();

  self.queue.push(function (next) {
    self._executeDirect(sql, function (err, result) {
      cb(err, result);

      return next();
    });
  });
};

odbc.ODBCStatement.prototype.executeNonQuery = function (params, cb) {
  var self = this;

  self.queue = self.queue || new SimpleQueue();

  if (!cb) {
    cb = params;
    params = null;
  }

  self.queue.push(function (next) {
    //If params were passed to this function, then bind them and
    //then executeNonQuery.
    if (params) {
      self._bind(params, function (err) {
        if (err) {
          return cb(err);
        }

        self._executeNonQuery(function (err, result) {
          cb(err, result);

          return next();
        });
      });
    }
    //Otherwise executeNonQuery and pop the next bind call
    else {
      self._executeNonQuery(function (err, result) {
        cb(err, result);

        //NOTE: We only execute the next queued bind call after
        // we have called execute() or executeNonQuery(). This ensures
        // that we don't call a bind() a bunch of times without ever
        // actually executing that bind. Not
        self.bindQueue && self.bindQueue.next();

        return next();
      });
    }
  });
};

odbc.ODBCStatement.prototype.prepare = function (sql, cb) {
  var self = this;

  self.queue = self.queue || new SimpleQueue();

  self.queue.push(function (next) {
    self._prepare(sql, function (err) {
      cb(err);

      return next();
    });
  });
};

odbc.ODBCStatement.prototype.bind = function (ary, cb) {
  var self = this;

  self.bindQueue = self.bindQueue || new SimpleQueue();

  self.bindQueue.push(function () {
    self._bind(ary, function (err) {
      cb(err);

      //NOTE: we do not call next() here because
      //we want to pop the next bind call only
      //after the next execute call
    });
  });
};


//proxy the ODBCResult fetch function so that it is queued
odbc.ODBCResult.prototype._fetch = odbc.ODBCResult.prototype.fetch;

odbc.ODBCResult.prototype.fetch = function (cb) {
  var self = this;

  self.queue = self.queue || new SimpleQueue();

  self.queue.push(function (next) {
    self._fetch(function (err, data) {
      if (cb) cb(err, data);

      return next();
    });
  });
};
