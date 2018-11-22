/*
  Copyright (c) 2013, Dan VerWeire<dverweire@gmail.com>

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

  SQL_ERROR Value is: -1
  SQL_SUCCESS Value is: 0
  SQL_SQL_SUCCESS_WITH_INFO Value is: 1
  SQL_NEED_DATA Value is: 99

*/

#include <time.h>
#include "utils.h"
#include "deferred_async_worker.h"
#include "odbc_statement.h"
#include "odbc_result.h"
#include "odbc.h"

Napi::FunctionReference ODBCStatement::constructor;

HENV ODBCStatement::m_hENV;
HDBC ODBCStatement::m_hDBC;
HSTMT ODBCStatement::m_hSTMT;

Napi::Object ODBCStatement::Init(Napi::Env env, Napi::Object exports) {

  DEBUG_PRINTF("ODBCStatement::Init\n");

  Napi::HandleScope scope(env);

  Napi::Function constructorFunction = DefineClass(env, "ODBCStatement", {

    InstanceMethod("executeDirect", &ODBCStatement::ExecuteDirect),
    InstanceMethod("executeNonQuery", &ODBCStatement::ExecuteNonQuery),
    InstanceMethod("prepare", &ODBCStatement::Prepare),
    InstanceMethod("bind", &ODBCStatement::Bind),
    InstanceMethod("execute", &ODBCStatement::Execute),
    InstanceMethod("close", &ODBCStatement::Close)
  });

  // Attach the Database Constructor to the target object
  constructor = Napi::Persistent(constructorFunction);
  constructor.SuppressDestruct();

  exports.Set("ODBCStatement", constructorFunction);

  return exports;
}

ODBCStatement::ODBCStatement(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ODBCStatement>(info) {

  this->data = new QueryData();

  this->m_hENV = *(info[0].As<Napi::External<SQLHENV>>().Data());
  this->m_hDBC = *(info[1].As<Napi::External<SQLHDBC>>().Data());
  this->data->hSTMT = *(info[2].As<Napi::External<SQLHSTMT>>().Data());
}

ODBCStatement::~ODBCStatement() {
  this->Free();
}

void ODBCStatement::Free() {

  DEBUG_PRINTF("ODBCStatement::Free\n");

  // delete data;

  if (m_hSTMT) {
    uv_mutex_lock(&ODBC::g_odbcMutex);
    SQLFreeHandle(SQL_HANDLE_STMT, m_hSTMT);
    m_hSTMT = NULL;
    uv_mutex_unlock(&ODBC::g_odbcMutex);
  }
}

/******************************************************************************
 **************************** EXECUTE NON QUERY *******************************
 *****************************************************************************/

// ExecuteNonQueryAsyncWorker, used by ExecuteNonQuery function (see below)
class ExecuteNonQueryAsyncWorker : public DeferredAsyncWorker {

  public:
    ExecuteNonQueryAsyncWorker(ODBCStatement *odbcStatementObject, Napi::Promise::Deferred deferred)
    :DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject), data(odbcStatementObject->data) {}

    ~ExecuteNonQueryAsyncWorker() {}

    void Execute() {
      DEBUG_PRINTF("ODBCStatement::ExecuteNonQueryAsyncWorker in Execute()\n");

      data->sqlReturnCode = SQLExecute(data->hSTMT);

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {

      } else {
        SetError("Error");
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCStatement::AsyncWorkerExecuteNonQuery in OnOk()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      SQLLEN rowCount = 0;

      data->sqlReturnCode = SQLRowCount(data->hSTMT, &rowCount);

      if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
        rowCount = 0;
      }

      uv_mutex_lock(&ODBC::g_odbcMutex);
      SQLFreeStmt(data->hSTMT, SQL_CLOSE);
      uv_mutex_unlock(&ODBC::g_odbcMutex);

      Resolve(Napi::Number::New(env, rowCount));
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCStatement::AsyncWorkerExecuteNonQuery in OnError()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::ExecuteNonQueryAsyncWorker"));

    }

  private:
    ODBCStatement *odbcStatementObject;
    QueryData *data;
};

/*
 *  ODBCStatement::ExecuteNonQuery (Async)
 *    Description: Executes a bound and prepared statement and returns only the
 *                 number of rows affected.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed by Napi from the JavaScript call, including
 *        arguments from the JavaScript function. In JavaScript, the
 *        fetchAll() function takes one argument.
 *
 *        info[0]: Function: callback function:
 *            function(error, result)
 *              error: An error object if there was a problem getting results,
 *                     or null if operation was successful.
 *              result: The number of rows affected by the executed query.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback).
 */
Napi::Value ODBCStatement::ExecuteNonQuery(const Napi::CallbackInfo& info) {
  DEBUG_PRINTF("ODBCStatement::ExecuteNonQuery\n");
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  ExecuteNonQueryAsyncWorker *worker = new ExecuteNonQueryAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ****************************** EXECUTE DIRECT ********************************
 *****************************************************************************/

// ExecuteDirectAsyncWorker, used by ExecuteDirect function (see below)
class ExecuteDirectAsyncWorker : public DeferredAsyncWorker {

  public:
    ExecuteDirectAsyncWorker(ODBCStatement *odbcStatementObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject), data(odbcStatementObject->data) {}

    ~ExecuteDirectAsyncWorker() {}

    void Execute() {
      DEBUG_PRINTF("\nODBCConnection::QueryAsyncWorke::Execute");

      DEBUG_PRINTF("ODBCConnection::Query : sqlLen=%i, sqlSize=%i, sql=%s\n",
               data->sqlLen, data->sqlSize, (char*)data->sql);

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

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {
        BindColumns(data);
      } else {
        SetError("Error");
      }
    }

    void OnOK() {
      DEBUG_PRINTF("ODBCConnection::QueryAsyncWorker::OnOk : data->sqlReturnCode=%i, data->noResultObject=%i\n", data->sqlReturnCode);

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      //check to see if the result set has columns
      if (data->columnCount == 0) {
        //this most likely means that the query was something like
        //'insert into ....'

        Resolve(env.Undefined());

      } else {

        std::vector<napi_value> resultArguments;
        resultArguments.push_back(Napi::External<HENV>::New(env, &(odbcStatementObject->m_hENV)));
        resultArguments.push_back(Napi::External<HDBC>::New(env, &(odbcStatementObject->m_hDBC)));
        resultArguments.push_back(Napi::External<HSTMT>::New(env, &(data->hSTMT)));
        resultArguments.push_back(Napi::Boolean::New(env, false)); // canFreeHandle

        resultArguments.push_back(Napi::External<QueryData>::New(env, data));

        // create a new ODBCResult object as a Napi::Value
        Napi::Value resultObject = ODBCResult::constructor.New(resultArguments);

        Resolve(resultObject);
      }
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCStatement::ExecuteDirectAsyncWorker in OnError()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::ExecuteDirectAsyncWorker"));
    }

  private:
    ODBCStatement *odbcStatementObject;
    QueryData *data;
};

/*
 *  ODBCStatement::ExecuteDirect (Async)
 *    Description: Directly executes a statement without preparing it and
 *                 binding parameters.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed by Napi from the JavaScript call, including
 *        arguments from the JavaScript function. In JavaScript, the
 *        fetchAll() function takes one argument.
 *
 *        info[0]: Function: callback function:
 *            function(error, result)
 *              error: An error object if there was a problem getting results,
 *                     or null if operation was successful.
 *              result: The number of rows affected by the executed query.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback).
 */
Napi::Value ODBCStatement::ExecuteDirect(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCStatement::ExecuteDirect\n");
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  REQ_STRO_ARG(0, sql);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  data->sql = NapiStringToSQLTCHAR(sql);

  ExecuteDirectAsyncWorker *worker = new ExecuteDirectAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();

}

/******************************************************************************
 ********************************* PREPARE ************************************
 *****************************************************************************/

// PrepareAsyncWorker, used by Prepare function (see below)
class PrepareAsyncWorker : public DeferredAsyncWorker {

  public:
    PrepareAsyncWorker(ODBCStatement *odbcStatementObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject), data(odbcStatementObject->data) {}

    ~PrepareAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCStatement::PrepareAsyncWorker in Execute()\n");

      DEBUG_PRINTF("ODBCStatement::PrepareAsyncWorker m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",
       odbcStatementObject->m_hENV,
       odbcStatementObject->m_hDBC,
       data->hSTMT
      );

      data->sqlReturnCode = SQLPrepare(
        data->hSTMT,
        data->sql,
        SQL_NTS
      );

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {
        return;
      } else {
        SetError("ERROR");
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCStatement::PrepareAsyncWorker in OnOk()\n");
      DEBUG_PRINTF("ODBCStatement::PrepareAsyncWorker m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",
       odbcStatementObject->m_hENV,
       odbcStatementObject->m_hDBC,
       data->hSTMT
      );

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Resolve(Napi::Boolean::New(env, true));
    }

    void OnError(const Napi::Error &e) {

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::PrepareAsyncWorker"));
    }

  private:
    ODBCStatement *odbcStatementObject;
    QueryData *data;
};

/*
 *  ODBCStatement:Prepare (Async)
 *    Description: Prepares an SQL string so that it can be bound with
 *                 parameters and then executed.
 *
 *    Parameters:
 *      const Napi::CallbackInfo& info:
 *        The information passed by Napi from the JavaScript call, including
 *        arguments from the JavaScript function. In JavaScript, the
 *        prepare() function takes two arguments.
 *
 *        info[0]: String: the SQL string to prepare.
 *        info[1]: Function: callback function:
 *            function(error, result)
 *              error: An error object if there was a problem getting results,
 *                     or null if operation was successful.
 *              result: The number of rows affected by the executed query.
 *
 *    Return:
 *      Napi::Value:
 *        Undefined (results returned in callback).
 */
Napi::Value ODBCStatement::Prepare(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCStatement::Prepare\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  REQ_STRO_ARG(0, sql);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  data->sql = NapiStringToSQLTCHAR(sql);

  PrepareAsyncWorker *worker = new PrepareAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 *********************************** BIND *************************************
 *****************************************************************************/

// BindAsyncWorker, used by Bind function (see below)
class BindAsyncWorker : public DeferredAsyncWorker {

  public:
    BindAsyncWorker(ODBCStatement *odbcStatementObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject), data(odbcStatementObject->data) {}

    ~BindAsyncWorker() { }

    void Execute() {

      printf("BindAsyncWorker::Execute\n");

      if (data->paramCount > 0) {
        // binds all parameters to the query
        BindParameters(data);
      }
    }

    void OnOK() {

      printf("BindAsyncWorker::OnOk\n");

      DEBUG_PRINTF("ODBCStatement::UV_AfterBind\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Resolve(Napi::Boolean::New(env, true));
    }

    void OnError(const Napi::Error &e) {

      printf("BindAsyncWorker::OnError\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::BindAsyncWorker"));
    }

  private:
    ODBCStatement *odbcStatementObject;
    QueryData *data;
};

Napi::Value ODBCStatement::Bind(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCStatement::Bind\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (!info[0].IsArray()) {
    Napi::Error::New(env, "Argument 1 must be an Array").ThrowAsJavaScriptException();
    return env.Null();
  }

  Napi::Array parameterArray = info[0].As<Napi::Array>();

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  this->data->params = GetParametersFromArray(&parameterArray, &(data->paramCount));

  BindAsyncWorker *worker = new BindAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}


/******************************************************************************
 ********************************* EXECUTE ************************************
 *****************************************************************************/

// ExecuteAsyncWorker, used by Execute function (see below)
class ExecuteAsyncWorker : public DeferredAsyncWorker {
  public:
    ExecuteAsyncWorker(ODBCStatement *odbcStatementObject, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject), data(odbcStatementObject->data) {}

    ~ExecuteAsyncWorker() {}

    void Execute() {

      DEBUG_PRINTF("ODBCStatement::ExecuteAsyncWorker::Execute()\n");

      data->sqlReturnCode = SQLExecute(data->hSTMT);

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {

        BindColumns(data);

      } else {
        SetError("ERROR");
      }
    }

    void OnOK() {

      DEBUG_PRINTF("ODBCStatement::ExecuteAsyncWorker::OnOk()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      std::vector<napi_value> resultArguments;

      resultArguments.push_back(Napi::External<HENV>::New(env, &(odbcStatementObject->m_hENV)));
      resultArguments.push_back(Napi::External<HDBC>::New(env, &(odbcStatementObject->m_hDBC)));
      resultArguments.push_back(Napi::External<HSTMT>::New(env, &(data->hSTMT)));
      resultArguments.push_back(Napi::Boolean::New(env, false)); // canFreeHandle

      resultArguments.push_back(Napi::External<QueryData>::New(env, data));

      // create a new ODBCResult object as a Napi::Value
      Napi::Value resultObject = ODBCResult::constructor.New(resultArguments);

      Resolve(resultObject);
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCStatement::ExecuteAsyncWorker::OnError()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);


      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::ExecuteAsyncWorker"));
    }

  private:
    ODBCStatement *odbcStatementObject;
    QueryData *data;
};

Napi::Value ODBCStatement::Execute(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCStatement::Execute\n");
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  ExecuteAsyncWorker *worker = new ExecuteAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}

/******************************************************************************
 ********************************** CLOSE *************************************
 *****************************************************************************/

// CloseAsyncWorker, used by Close function (see below)
class CloseAsyncWorker : public DeferredAsyncWorker {
  public:
    CloseAsyncWorker(ODBCStatement *odbcStatementObject, int closeOption, Napi::Promise::Deferred deferred)
    : DeferredAsyncWorker(deferred), odbcStatementObject(odbcStatementObject),
        closeOption(closeOption), data(odbcStatementObject->data) {}

    ~CloseAsyncWorker() {}

    void Execute() {

      if (closeOption == SQL_DESTROY) {
        odbcStatementObject->Free();
      } else {
        uv_mutex_lock(&ODBC::g_odbcMutex);
        data->sqlReturnCode = SQLFreeStmt(odbcStatementObject->m_hSTMT, closeOption);
        uv_mutex_unlock(&ODBC::g_odbcMutex);
      }

      if (SQL_SUCCEEDED(data->sqlReturnCode)) {
        return;
      } else {
        SetError("Error");
      }

      DEBUG_PRINTF("ODBCStatement::CloseAsyncWorker::Execute()\n");
    }

    void OnError(const Napi::Error &e) {

      DEBUG_PRINTF("ODBCStatement::CloseAsyncWorker::OnError()\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      Reject(GetSQLError(env, SQL_HANDLE_STMT, data->hSTMT,
            (char *) "[node-odbc] Error in ODBCStatement::CloseAsyncWorker"));
    }

  private:
    ODBCStatement *odbcStatementObject;
    int closeOption;
    QueryData *data;
};

Napi::Value ODBCStatement::Close(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBCStatement::Close\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (info.Length() == 0 || !info[0].IsNumber()) {
    Napi::TypeError::New(env, "close take 1 argument (closeOption [int])").ThrowAsJavaScriptException();
  }

  int closeOption = info[0].ToNumber().Int32Value();

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  CloseAsyncWorker *worker = new CloseAsyncWorker(this, closeOption, deferred);
  worker->Queue();

  return deferred.Promise();
}
