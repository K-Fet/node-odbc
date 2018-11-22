/*
  Copyright (c) 2013, Dan VerWeire <dverweire@gmail.com>
  Copyright (c) 2010, Lee Smith<notwink@gmail.com>

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
#include "odbc_connection.h"
#include "utils.h"
#include "deferred_async_worker.h"
#include "odbc_statement.h"
#include "odbc_result.h"
#include <time.h>

Napi::FunctionReference ODBCConnection::constructor;

Napi::String ODBCConnection::OPTION_SQL;
Napi::String ODBCConnection::OPTION_PARAMS;
Napi::String ODBCConnection::OPTION_NORESULTS;

Napi::Object ODBCConnection::Init(Napi::Env env, Napi::Object exports) {

  DEBUG_PRINTF("ODBCConnection::Init\n");
  Napi::HandleScope scope(env);

  OPTION_SQL = Napi::String::New(env, "sql");
  OPTION_PARAMS = Napi::String::New(env, "params");
  OPTION_NORESULTS = Napi::String::New(env, "noResults");

  Napi::Function constructorFunction = DefineClass(env, "ODBCConnection", {
    InstanceMethod("open", &ODBCConnection::Open),
    InstanceMethod("close", &ODBCConnection::Close),
    InstanceMethod("createStatement", &ODBCConnection::CreateStatement),
    InstanceMethod("query", &ODBCConnection::Query),
    InstanceMethod("beginTransaction", &ODBCConnection::BeginTransaction),
    InstanceMethod("endTransaction", &ODBCConnection::EndTransaction),
    InstanceMethod("getInfo", &ODBCConnection::GetInfo),
    InstanceMethod("columns", &ODBCConnection::Columns),
    InstanceMethod("tables", &ODBCConnection::Tables),

    InstanceAccessor("connected", &ODBCConnection::ConnectedGetter, nullptr),
    InstanceAccessor("connectTimeout", &ODBCConnection::ConnectTimeoutGetter, &ODBCConnection::ConnectTimeoutSetter),
    InstanceAccessor("loginTimeout", &ODBCConnection::LoginTimeoutGetter, &ODBCConnection::LoginTimeoutSetter)
  });

  constructor = Napi::Persistent(constructorFunction);
  constructor.SuppressDestruct();

  exports.Set("ODBCConnection", constructorFunction);

  return exports;
}

ODBCConnection::ODBCConnection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ODBCConnection>(info) {

  this->m_hENV = *(info[0].As<Napi::External<SQLHENV>>().Data());
  this->m_hDBC = *(info[1].As<Napi::External<SQLHDBC>>().Data());

  //set default connectTimeout to 0 seconds
  this->connectTimeout = 0;
  //set default loginTimeout to 5 seconds
  this->loginTimeout = 5;

}

ODBCConnection::~ODBCConnection() {

  DEBUG_PRINTF("ODBCConnection::~ODBCConnection\n");
  SQLRETURN sqlReturnCode;

  this->Free(&sqlReturnCode);
}

void ODBCConnection::Free(SQLRETURN *sqlReturnCode) {

  DEBUG_PRINTF("ODBCConnection::Free\n");

  uv_mutex_lock(&ODBC::g_odbcMutex);

    if (m_hDBC) {
      SQLDisconnect(m_hDBC);
      SQLFreeHandle(SQL_HANDLE_DBC, m_hDBC);
      m_hDBC = NULL;
    }

  uv_mutex_unlock(&ODBC::g_odbcMutex);

  return;

  // TODO: I think this is the ODBC workflow to close a connection.
  //       But I think we have to check statements first.
  //       Maybe keep a list of open statements?
  // if (this->m_hDBC) {

  //   uv_mutex_lock(&ODBC::g_odbcMutex);

  //   // If an application calls SQLDisconnect before it has freed all statements
  //   // associated with the connection, the driver, after it successfully
  //   // disconnects from the data source, frees those statements and all
  //   // descriptors that have been explicitly allocated on the connection.
  //   *sqlReturnCode = SQLDisconnect(this->m_hDBC);

  //   printf("\nDisconnected hDBC = %d", *sqlReturnCode);

  //   if (SQL_SUCCEEDED(*sqlReturnCode)) {

  //     //Before it calls SQLFreeHandle with a HandleType of SQL_HANDLE_DBC, an
  //     //application must call SQLDisconnect for the connection if there is a
  //     // connection on this handle. Otherwise, the call to SQLFreeHandle
  //     //returns SQL_ERROR and the connection remains valid.
  //     *sqlReturnCode = SQLFreeHandle(SQL_HANDLE_DBC, m_hDBC);

  //     printf("\nFree handle hDBC = %d", *sqlReturnCode);

  //     if (SQL_SUCCEEDED(*sqlReturnCode)) {

  //       m_hDBC = NULL;
  //       this->connected = false;

  //     } else {
  //       uv_mutex_unlock(&ODBC::g_odbcMutex);
  //       return;
  //     }

  //   } else {
  //     uv_mutex_unlock(&ODBC::g_odbcMutex);
  //     return;
  //   }

  //   uv_mutex_unlock(&ODBC::g_odbcMutex);
  //   return;
  // }
}

Napi::Value ODBCConnection::ConnectedGetter(const Napi::CallbackInfo& info) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  return Napi::Boolean::New(env, this->connected);
}

Napi::Value ODBCConnection::ConnectTimeoutGetter(const Napi::CallbackInfo& info) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  return Napi::Number::New(env, this->connectTimeout);
}

void ODBCConnection::ConnectTimeoutSetter(const Napi::CallbackInfo& info, const Napi::Value& value) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (value.IsNumber()) {
    this->connectTimeout = value.As<Napi::Number>().Uint32Value();
  }
}

Napi::Value ODBCConnection::LoginTimeoutGetter(const Napi::CallbackInfo& info) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  return Napi::Number::New(env, this->loginTimeout);
}

void ODBCConnection::LoginTimeoutSetter(const Napi::CallbackInfo& info, const Napi::Value& value) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (value.IsNumber()) {
    this->loginTimeout = value.As<Napi::Number>().Uint32Value();
  }
}


/******************************************************************************
 *********************************** OPEN *************************************
 *****************************************************************************/

 // OpenAsyncWorker, used by Open function (see below)
class OpenAsyncWorker : public DeferredAsyncWorker {

  public:
    OpenAsyncWorker(ODBCConnection *odbcConnectionObject, SQLTCHAR *connectionStringPtr, Napi::Promise::Deferred deferred)
     : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject),
            connectionStringPtr(connectionStringPtr) {}

    ~OpenAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCConnection::OpenAsyncWorker::Execute : connectTimeout=%i, loginTimeout = %i\n",
        *&(odbcConnectionObject->connectTimeout), *&(odbcConnectionObject->loginTimeout));

      uv_mutex_lock(&ODBC::g_odbcMutex);

      if (odbcConnectionObject->connectTimeout > 0) {
        //NOTE: SQLSetConnectAttr requires the thread to be locked
        sqlReturnCode = SQLSetConnectAttr(
          odbcConnectionObject->m_hDBC,                              // ConnectionHandle
          SQL_ATTR_CONNECTION_TIMEOUT,                               // Attribute
          (SQLPOINTER) size_t(odbcConnectionObject->connectTimeout), // ValuePtr
          SQL_IS_UINTEGER);                                          // StringLength
      }

      if (odbcConnectionObject->loginTimeout > 0) {
        //NOTE: SQLSetConnectAttr requires the thread to be locked
        sqlReturnCode = SQLSetConnectAttr(
          odbcConnectionObject->m_hDBC,                            // ConnectionHandle
          SQL_ATTR_LOGIN_TIMEOUT,                                  // Attribute
          (SQLPOINTER) size_t(odbcConnectionObject->loginTimeout), // ValuePtr
          SQL_IS_UINTEGER);                                        // StringLength
      }

      //Attempt to connect
      //NOTE: SQLDriverConnect requires the thread to be locked
      sqlReturnCode = SQLDriverConnect(
        odbcConnectionObject->m_hDBC, // ConnectionHandle
        NULL,                         // WindowHandle
        connectionStringPtr,             // InConnectionString
        SQL_NTS,                      // StringLength1
        NULL,                         // OutConnectionString
        0,                            // BufferLength - in characters
        NULL,                         // StringLength2Ptr
        SQL_DRIVER_NOPROMPT);         // DriverCompletion

      if (SQL_SUCCEEDED(sqlReturnCode)) {

        odbcConnectionObject->connected = true;

        HSTMT hStmt;

        //allocate a temporary statment
        sqlReturnCode = SQLAllocHandle(SQL_HANDLE_STMT, odbcConnectionObject->m_hDBC, &hStmt);

        //try to determine if the driver can handle
        //multiple recordsets
        sqlReturnCode = SQLGetFunctions(
          odbcConnectionObject->m_hDBC,
          SQL_API_SQLMORERESULTS,
          &(odbcConnectionObject->canHaveMoreResults));

        if (!SQL_SUCCEEDED(sqlReturnCode)) {
          odbcConnectionObject->canHaveMoreResults = 0;
        }

        //free the handle
        sqlReturnCode = SQLFreeHandle(SQL_HANDLE_STMT, hStmt);

      } else {
        SetError("null");
      }

      uv_mutex_unlock(&ODBC::g_odbcMutex);
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCConnection::OpenAsyncWorker::OnOK\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      odbcConnectionObject->connected = true;

      Resolve(env.Undefined());
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCConnection::OpenAsyncWorker::OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::OpenAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLTCHAR *connectionStringPtr;
    SQLRETURN sqlReturnCode;
};

/*
 *  ODBCConnection::Open
 *
 *    Description: Open a connection to a database
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'open'.
 *
 *        info[0]: String: connection string
 *        info[1]: Function: callback function:
 *            function(error)
 *              error: An error object if the connection was not opened, or
 *                     null if operation was successful.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::Open(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::Open\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  SQLTCHAR *connectionString = NapiStringToSQLTCHAR(info[0].As<Napi::String>());

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  OpenAsyncWorker *worker = new OpenAsyncWorker(this, connectionString, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ********************************** CLOSE *************************************
 *****************************************************************************/

// CloseAsyncWorker, used by Close function (see below)
class CloseAsyncWorker : public DeferredAsyncWorker {

  public:
    CloseAsyncWorker(ODBCConnection *odbcConnectionObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject) {}

    ~CloseAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCConnection::CloseAsyncWorker::Execute\n");

      odbcConnectionObject->Free(&sqlReturnCode);

      if (!SQL_SUCCEEDED(sqlReturnCode)) {
        SetError("ERROR");
      }
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCConnection::CloseAsyncWorker::OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::CloseAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLRETURN sqlReturnCode;
};

/*
 *  ODBCConnection::Close (Async)
 *
 *    Description: Closes the connection asynchronously.
 *
 *    Parameters:
 *
 *      const Napi::CallbackInfo& info:
 *        The information passed by Napi from the JavaScript call, including
 *        arguments from the JavaScript function.
 *
 *         info[0]: Function: callback function, in the following format:
 *            function(error)
 *              error: An error object if the connection was not closed, or
 *                     null if operation was successful.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined. (The return values are attached to the callback function).
 */
Napi::Value ODBCConnection::Close(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::Close\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  CloseAsyncWorker *worker = new CloseAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}


/******************************************************************************
 ***************************** CREATE STATEMENT *******************************
 *****************************************************************************/

// CreateStatementAsyncWorker, used by CreateStatement function (see below)
class CreateStatementAsyncWorker : public DeferredAsyncWorker {

  public:
    CreateStatementAsyncWorker(ODBCConnection *odbcConnectionObject, Napi::Promise::Deferred deferred)
     : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject) {}

    ~CreateStatementAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCConnection::CreateStatementAsyncWorker:Execute - m_hDBC=%X m_hDBC=%X\n",
       odbcConnectionObject->m_hENV,
       odbcConnectionObject->m_hDBC,
      );

      uv_mutex_lock(&ODBC::g_odbcMutex);
      sqlReturnCode = SQLAllocHandle( SQL_HANDLE_STMT, odbcConnectionObject->m_hDBC, &hSTMT);
      uv_mutex_unlock(&ODBC::g_odbcMutex);


      if (SQL_SUCCEEDED(sqlReturnCode)) {
        return;
      } else {
        SetError("ERROR");
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCConnection::CreateStatementAsyncWorker::OnOK - m_hDBC=%X m_hDBC=%X hSTMT=%X\n",
        odbcConnectionObject->m_hENV,
        odbcConnectionObject->m_hDBC,
        hSTMT
      );

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      // arguments for the ODBCStatement constructor
      std::vector<napi_value> statementArguments;
      statementArguments.push_back(Napi::External<HENV>::New(env, &(odbcConnectionObject->m_hENV)));
      statementArguments.push_back(Napi::External<HDBC>::New(env, &(odbcConnectionObject->m_hDBC)));
      statementArguments.push_back(Napi::External<HSTMT>::New(env, &hSTMT));

      // create a new ODBCStatement object as a Napi::Value
      Napi::Value statementObject = ODBCStatement::constructor.New(statementArguments);

      Resolve(statementObject);
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCConnection::CreateStatementAsyncWorker::OnError - m_hDBC=%X m_hDBC=%X hSTMT=%X\n",
        odbcConnectionObject->m_hENV,
        odbcConnectionObject->m_hDBC,
        hSTMT
      );

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::CreateStatementAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLRETURN sqlReturnCode;
    HSTMT hSTMT;
};

/*
 *  ODBCConnection::CreateStatement
 *
 *    Description: Create an ODBCStatement to manually prepare, bind, and
 *                 execute.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'endTransactionSync'.
 *
 *        info[0]: Function: callback function:
 *            function(error, statement)
 *              error: An error object if there was an error creating the
 *                     statement, or null if operation was successful.
 *              statement: The newly created ODBCStatement object
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::CreateStatement(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::CreateStatement\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  CreateStatementAsyncWorker *worker = new CreateStatementAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ********************************** QUERY *************************************
 *****************************************************************************/

// QueryAsyncWorker, used by Query function (see below)
class QueryAsyncWorker : public DeferredAsyncWorker {

  public:
    QueryAsyncWorker(ODBCConnection *odbcConnectionObject, QueryData *data, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject), data(data) {}

    ~QueryAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("\nODBCConnection::QueryAsyncWorke::Execute");

      DEBUG_PRINTF("ODBCConnection::Query : sqlLen=%i, sqlSize=%i, sql=%s\n",
               data->sqlLen, data->sqlSize, (char*)data->sql);

      // allocate a new statement handle
      uv_mutex_lock(&ODBC::g_odbcMutex);
      data->sqlReturnCode = SQLAllocHandle(SQL_HANDLE_STMT, odbcConnectionObject->m_hDBC, &(data->hSTMT));
      uv_mutex_unlock(&ODBC::g_odbcMutex);

      if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
        return;
      }

      if (data->paramCount > 0) {
        // binds all parameters to the query
        BindParameters(data);
      }

      // execute the query directly
      data->sqlReturnCode = SQLExecDirect(
        data->hSTMT,
        data->sql,
        SQL_NTS
      );

      if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
        SetError("ERROR");
        return;
      } else {
        BindColumns(data);
        return;
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCConnection::QueryAsyncWorker::OnOk : data->sqlReturnCode=%i\n", data->sqlReturnCode);

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      // no result object should be created, just return with true instead
      if (data->noResultObject) {

        uv_mutex_lock(&ODBC::g_odbcMutex);

        SQLFreeHandle(SQL_HANDLE_STMT, data->hSTMT);

        uv_mutex_unlock(&ODBC::g_odbcMutex);

        Resolve(Napi::Boolean::New(env, true));
      } else {
        // arguments for the ODBCResult constructor
        std::vector<napi_value> resultArguments;
        resultArguments.push_back(Napi::External<HENV>::New(env, &(odbcConnectionObject->m_hENV)));
        resultArguments.push_back(Napi::External<HDBC>::New(env, &(odbcConnectionObject->m_hDBC)));
        resultArguments.push_back(Napi::External<HSTMT>::New(env, &(data->hSTMT)));
        resultArguments.push_back(Napi::Boolean::New(env, true)); // canFreeHandle
        resultArguments.push_back(Napi::External<QueryData>::New(env, data));

        // create a new ODBCResult object as a Napi::Value
        Napi::Value resultObject = ODBCResult::constructor.New(resultArguments);

        Resolve(resultObject);
      }
    }

    void OnError(const Napi::Error &e) {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCConnection::QueryAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    QueryData      *data;
};

/*
 *  ODBCConnection::Query
 *
 *    Description: Returns the info requested from the connection.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'query'.
 *
 *        info[0]: String: the SQL string to execute
 *        info[1?]: Array: optional array of parameters to bind to the query
 *        info[1/2]: Function: callback function:
 *            function(error, result)
 *              error: An error object if the connection was not opened, or
 *                     null if operation was successful.
 *              result: A string containing the info requested.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::Query(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::Query\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  QueryData *data = new QueryData;

  Napi::String sql = info[0].ToString();

  // check if parameters were passed or not
  if (info.Length() == 3 && info[1].IsArray()) {
    Napi::Array parameterArray = info[1].As<Napi::Array>();
    data->params = GetParametersFromArray(&parameterArray, &(data->paramCount));
  } else {
    data->params = 0;
  }

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  data->sql = NapiStringToSQLTCHAR(sql);

  // DEBUG_PRINTF("ODBCConnection::Query : sqlLen=%i, sqlSize=%i, sql=%s\n",
  //              data->sqlLen, data->sqlSize, (char*)data->sql);

  QueryAsyncWorker *worker = new QueryAsyncWorker(this, data, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ******************************** GET INFO ************************************
 *****************************************************************************/

// GetInfoAsyncWorker, used by GetInfo function (see below)
class GetInfoAsyncWorker : public DeferredAsyncWorker {

  public:
    GetInfoAsyncWorker(ODBCConnection *odbcConnectionObject, SQLUSMALLINT infoType, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject), infoType(infoType) {}

    ~GetInfoAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCConnection::GetInfoAsyncWorker:Execute");

      switch (infoType) {
        case SQL_USER_NAME:

          sqlReturnCode = SQLGetInfo(odbcConnectionObject->m_hDBC, SQL_USER_NAME, userName, sizeof(userName), &userNameLength);

          if (SQL_SUCCEEDED(sqlReturnCode)) {
           return;
          } else {
            SetError("Error");
          }

        default:
          SetError("Error");
      }
    }

    void OnOK() {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Napi::Value res;

      #ifdef UNICODE
        res = Napi::String::New(env, (const char16_t *)userName);
      #else
        res = Napi::String::New(env, (const char *) userName);
      #endif

      Resolve(res);
    }

    void OnError(const Napi::Error &e) {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::GetInfoAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLUSMALLINT infoType;
    SQLTCHAR userName[255];
    SQLSMALLINT userNameLength;
    SQLRETURN sqlReturnCode;
};

/*
 *  ODBCConnection::GetInfo
 *
 *    Description: Returns the info requested from the connection.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'getInfo'.
 *
 *        info[0]: Number: option
 *        info[4]: Function: callback function:
 *            function(error, result)
 *              error: An error object if the connection was not opened, or
 *                     null if operation was successful.
 *              result: A string containing the info requested.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::GetInfo(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::GetInfo\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if ( !info[0].IsNumber() ) {
    Napi::TypeError::New(env, "ODBCConnection::GetInfo(): Argument 0 must be a Number.").ThrowAsJavaScriptException();
    return env.Null();
  }

  SQLUSMALLINT infoType = info[0].As<Napi::Number>().Int32Value();

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  GetInfoAsyncWorker *worker = new GetInfoAsyncWorker(this, infoType, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ********************************** TABLES ************************************
 *****************************************************************************/

// TablesAsyncWorker, used by Tables function (see below)
class TablesAsyncWorker : public DeferredAsyncWorker {

  public:
    TablesAsyncWorker(ODBCConnection *odbcConnectionObject, QueryData *data, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject), data(data) {}

    ~TablesAsyncWorker() {}

    void Execute() {

      uv_mutex_lock(&ODBC::g_odbcMutex);
      SQLAllocHandle(SQL_HANDLE_STMT, odbcConnectionObject->m_hDBC, &data->hSTMT );
      uv_mutex_unlock(&ODBC::g_odbcMutex);

      data->sqlReturnCode = SQLTables(
        data->hSTMT,
        data->catalog, SQL_NTS,
        data->schema, SQL_NTS,
        data->table, SQL_NTS,
        data->type, SQL_NTS
      );

      if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
        SetError("ERROR");
      } else {
        BindColumns(data);
        return;
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCConnection::QueryAsyncWorker::OnOk : data->sqlReturnCode=%i, \n", data->sqlReturnCode, );

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      // return results here
      //check to see if the result set has columns
      if (data->columnCount == 0) {
        //this most likely means that the query was something like
        //'insert into ....'
        Resolve(env.Undefined());
      } else {

        // arguments for the ODBCResult constructor
        std::vector<napi_value> resultArguments;

        resultArguments.push_back(Napi::External<HENV>::New(env, &(odbcConnectionObject->m_hENV)));
        resultArguments.push_back(Napi::External<HDBC>::New(env, &(odbcConnectionObject->m_hDBC)));
        resultArguments.push_back(Napi::External<HSTMT>::New(env, &(data->hSTMT)));
        resultArguments.push_back(Napi::Boolean::New(env, true)); // canFreeHandle
        resultArguments.push_back(Napi::External<QueryData>::New(env, data));

        // create a new ODBCResult object as a Napi::Value
        Napi::Value resultObject = ODBCResult::constructor.New(resultArguments);

        Resolve(resultObject);
      }
    }

    void OnError(const Napi::Error &e) {

      printf("OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::TablesAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    QueryData *data;
};

/*
 *  ODBCConnection::Tables
 *
 *    Description: Returns the list of table, catalog, or schema names, and
 *                 table types, stored in a specific data source.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'tables'.
 *
 *        info[0]: String: catalog
 *        info[1]: String: schema
 *        info[2]: String: table
 *        info[3]: String: type
 *        info[4]: Function: callback function:
 *            function(error, result)
 *              error: An error object if there was a database issue
 *              result: The ODBCResult object or a Boolean if noResultObject is
 *                      specified.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::Tables(const Napi::CallbackInfo& info) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length() != 4) {
    Napi::Error::New(env, "tables() function takes 4 arguments.").ThrowAsJavaScriptException();
  }

  Napi::String catalog = info[0].IsNull() ? Napi::String(env, env.Null()) : info[0].ToString();
  Napi::String schema = info[1].IsNull() ? Napi::String(env, env.Null()) : info[1].ToString();
  Napi::String table = info[2].IsNull() ? Napi::String(env, env.Null()) : info[2].ToString();
  Napi::String type = info[3].IsNull() ? Napi::String(env, env.Null()) : info[3].ToString();

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  QueryData* data = new QueryData();

  // Napi doesn't have LowMemoryNotification like NAN did. Throw standard error.
  if (!data) {
    Napi::Error::New(env, "Could not allocate enough memory").ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!catalog.IsNull()) { data->catalog = NapiStringToSQLTCHAR(catalog); }
  if (!schema.IsNull()) { data->schema = NapiStringToSQLTCHAR(schema); }
  if (!table.IsNull()) { data->table = NapiStringToSQLTCHAR(table); }
  if (!type.IsNull()) { data->type = NapiStringToSQLTCHAR(type); }

  TablesAsyncWorker *worker = new TablesAsyncWorker(this, data, deferred);
  worker->Queue();

  return deferred.Promise();
}


/******************************************************************************
 ********************************* COLUMNS ************************************
 *****************************************************************************/

// ColumnsAsyncWorker, used by Columns function (see below)
class ColumnsAsyncWorker : public DeferredAsyncWorker {

  public:
    ColumnsAsyncWorker(ODBCConnection *odbcConnectionObject, QueryData *data, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject), data(data) {}

    ~ColumnsAsyncWorker() {}

    void Execute() {

      uv_mutex_lock(&ODBC::g_odbcMutex);

      SQLAllocHandle(SQL_HANDLE_STMT, odbcConnectionObject->m_hDBC, &data->hSTMT );

      uv_mutex_unlock(&ODBC::g_odbcMutex);

      data->sqlReturnCode = SQLColumns(
        data->hSTMT,
        data->catalog, SQL_NTS,
        data->schema, SQL_NTS,
        data->table, SQL_NTS,
        data->column, SQL_NTS
      );

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {

        // manipulates the fields of QueryData object, which can then be fetched
        BindColumns(data);

        //Only loop through the recordset if there are columns
        if (data->columnCount > 0) {
          FetchAllData(data); // fetches all data and puts it in data->storedRows
        }

        if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
          SetError("ERROR");
        }

      } else {
        SetError("ERROR");
      }
    }

    void OnOK() {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      // no result object should be created, just return with true instead
      if (data->noResultObject) {

        //free the handle
        data->sqlReturnCode = SQLFreeHandle(SQL_HANDLE_STMT, data->hSTMT);

        Resolve(Napi::Boolean::New(env, true));

      } else {

        // arguments for the ODBCResult constructor
        std::vector<napi_value> resultArguments;
        resultArguments.push_back(Napi::External<HENV>::New(env, &(odbcConnectionObject->m_hENV)));
        resultArguments.push_back(Napi::External<HDBC>::New(env, &(odbcConnectionObject->m_hDBC)));
        resultArguments.push_back(Napi::External<HSTMT>::New(env, &(data->hSTMT)));
        resultArguments.push_back(Napi::Boolean::New(env, true)); // canFreeHandle

        resultArguments.push_back(Napi::External<QueryData>::New(env, data));

        // create a new ODBCResult object as a Napi::Value
        Napi::Value resultObject = ODBCResult::constructor.New(resultArguments);

        Resolve(resultObject);
      }
    }

    void OnError(const Napi::Error &e) {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCConnection::ColumnsAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    QueryData *data;
};

/*
 *  ODBCConnection::Columns
 *
 *    Description: Returns the list of column names in specified tables.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'columns'.
 *
 *        info[0]: String: catalog
 *        info[1]: String: schema
 *        info[2]: String: table
 *        info[3]: String: type
 *        info[4]: Function: callback function:
 *            function(error, result)
 *              error: An error object if there was a database error
 *              result: The ODBCResult object or a Boolean if noResultObject is
 *                      specified.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback)
 */
Napi::Value ODBCConnection::Columns(const Napi::CallbackInfo& info) {

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  // check arguments
  REQ_STRO_OR_NULL_ARG(0, catalog);
  REQ_STRO_OR_NULL_ARG(1, schema);
  REQ_STRO_OR_NULL_ARG(2, table);
  REQ_STRO_OR_NULL_ARG(3, type);

  QueryData* data = new QueryData;

  // Napi doesn't have LowMemoryNotification like NAN did. Throw standard error.
  if (!data) {
    Napi::Error::New(env, "Could not allocate enough memory").ThrowAsJavaScriptException();
    return env.Null();
  }

  if (!catalog.IsNull()) { data->catalog = NapiStringToSQLTCHAR(catalog); }
  if (!schema.IsNull()) { data->schema = NapiStringToSQLTCHAR(schema); }
  if (!table.IsNull()) { data->table = NapiStringToSQLTCHAR(table); }
  if (!type.IsNull()) { data->type = NapiStringToSQLTCHAR(type); }

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  ColumnsAsyncWorker *worker = new ColumnsAsyncWorker(this, data, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 **************************** BEGIN TRANSACTION *******************************
 *****************************************************************************/

// BeginTransactionAsyncWorker, used by EndTransaction function (see below)
class BeginTransactionAsyncWorker : public DeferredAsyncWorker {

  public:
    BeginTransactionAsyncWorker(ODBCConnection *odbcConnectionObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject) {}

    ~BeginTransactionAsyncWorker() {}

    void Execute() {

        DEBUG_PRINTF("ODBCConnection::BeginTransactionAsyncWorker::Execute\n");

        //set the connection manual commits
        sqlReturnCode = SQLSetConnectAttr(odbcConnectionObject->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_OFF, SQL_NTS);

        if (SQL_SUCCEEDED(sqlReturnCode)) {
          return;
        } else {
          SetError("Error");
        }
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCConnection::BeginTransactionAsyncWorker::OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::BeginTransactionAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLRETURN sqlReturnCode;
};

/*
 *  ODBCConnection::BeginTransaction (Async)
 *
 *    Description: Begin a transaction by turning off SQL_ATTR_AUTOCOMMIT.
 *                 Transaction is commited or rolledback in EndTransaction or
 *                 EndTransactionSync.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'beginTransaction'.
 *
 *        info[0]: Function: callback function:
 *            function(error)
 *              error: An error object if the transaction wasn't started, or
 *                     null if operation was successful.
 *
 *    Return:
 *      Napi::Value:
 *        Boolean, indicates whether the transaction was successfully started
 */
Napi::Value ODBCConnection::BeginTransaction(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::BeginTransaction\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  BeginTransactionAsyncWorker *worker = new BeginTransactionAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}


/******************************************************************************
 ***************************** END TRANSACTION ********************************
 *****************************************************************************/

 // EndTransactionAsyncWorker, used by EndTransaction function (see below)
class EndTransactionAsyncWorker : public DeferredAsyncWorker {

  public:
    EndTransactionAsyncWorker(ODBCConnection *odbcConnectionObject, SQLSMALLINT completionType, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcConnectionObject(odbcConnectionObject), completionType(completionType) {}

    ~EndTransactionAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCConnection::EndTransactionAsyncWorker::Execute\n");

      //Call SQLEndTran
      sqlReturnCode = SQLEndTran(SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC, completionType);

      if (SQL_SUCCEEDED(sqlReturnCode)) {

        //Reset the connection back to autocommit
        sqlReturnCode = SQLSetConnectAttr(odbcConnectionObject->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, SQL_NTS);

        if (SQL_SUCCEEDED(sqlReturnCode)) {
          return;
        } else {
          SetError("ERROR");
        }
      } else {
        SetError("ERROR");
      }
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCConnection::EndTransactionAsyncWorker::OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_DBC, odbcConnectionObject->m_hDBC,
            (char *) "[node-odbc] Error in ODBCConnection::EndTransactionAsyncWorker"));
    }

  private:
    ODBCConnection *odbcConnectionObject;
    SQLSMALLINT completionType;
    SQLRETURN sqlReturnCode;
};

/*
 *  ODBCConnection::EndTransaction (Async)
 *
 *    Description: Ends a transaction by calling SQLEndTran on the connection
 *                 in an AsyncWorker.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed from the JavaSript environment, including the
 *        function arguments for 'endTransaction'.
 *
 *        info[0]: Boolean: whether to rollback (true) or commit (false)
 *        info[1]: Function: callback function:
 *            function(error)
 *              error: An error object if the transaction wasn't ended, or
 *                     null if operation was successful.
 *
 *    Return:
 *      Napi::Value:
 *        Boolean, indicates whether the transaction was successfully ended
 */
Napi::Value ODBCConnection::EndTransaction(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCConnection::EndTransaction\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  REQ_BOOL_ARG(0, rollback);

  SQLSMALLINT completionType = rollback.Value() ? SQL_ROLLBACK : SQL_COMMIT;

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  EndTransactionAsyncWorker *worker = new EndTransactionAsyncWorker(this, completionType, deferred);
  worker->Queue();

  return deferred.Promise();
}
