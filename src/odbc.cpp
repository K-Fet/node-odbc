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

#include <napi.h>
#include <time.h>

#include "odbc.h"
#include "utils.h"
#include "deferred_async_worker.h"
#include "odbc_connection.h"

#ifdef dynodbc
#include "dynodbc.h"
#endif

uv_mutex_t ODBC::g_odbcMutex;

Napi::FunctionReference ODBC::constructor;

Napi::Object ODBC::Init(Napi::Env env, Napi::Object exports) {
  DEBUG_PRINTF("ODBC::Init\n");
  Napi::HandleScope scope(env);

  Napi::Function constructorFunction = DefineClass(env, "ODBC", {
    InstanceMethod("createConnection", &ODBC::CreateConnection),

    // instance values [THESE WERE 'constant_attributes' in NAN, is there an equivalent here?]
    StaticValue("SQL_CLOSE", Napi::Number::New(env, SQL_CLOSE)),
    StaticValue("SQL_DROP", Napi::Number::New(env, SQL_DROP)),
    StaticValue("SQL_UNBIND", Napi::Number::New(env, SQL_UNBIND)),
    StaticValue("SQL_RESET_PARAMS", Napi::Number::New(env, SQL_RESET_PARAMS)),
    StaticValue("SQL_DESTROY", Napi::Number::New(env, SQL_DESTROY)),
    StaticValue("SQL_USER_NAME", Napi::Number::New(env, SQL_USER_NAME))
  });

  constructor = Napi::Persistent(constructorFunction);
  constructor.SuppressDestruct();

  exports.Set("ODBC", constructorFunction);

  // Initialize the cross platform mutex provided by libuv
  uv_mutex_init(&ODBC::g_odbcMutex);

  return exports;
}

ODBC::ODBC(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ODBC>(info) {
  DEBUG_PRINTF("ODBC::New\n");

  Napi::Env env = info.Env();

  this->m_hEnv = NULL;

  uv_mutex_lock(&ODBC::g_odbcMutex);

  // Initialize the Environment handle
  int ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_hEnv);

  uv_mutex_unlock(&ODBC::g_odbcMutex);

  if (!SQL_SUCCEEDED(ret)) {

    DEBUG_PRINTF("ODBC::New - ERROR ALLOCATING ENV HANDLE!!\n");

    Napi::Error(env, GetSQLError(env, SQL_HANDLE_ENV, this->m_hEnv)).ThrowAsJavaScriptException();
    return;
  }

  // Use ODBC 3.x behavior
  SQLSetEnvAttr(this->m_hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);
}

ODBC::~ODBC() {
  DEBUG_PRINTF("ODBC::~ODBC\n");
  this->Free();
}

void ODBC::Free() {
  DEBUG_PRINTF("ODBC::Free\n");
  uv_mutex_lock(&ODBC::g_odbcMutex);

  if (m_hEnv) {
    SQLFreeHandle(SQL_HANDLE_ENV, m_hEnv);
    m_hEnv = NULL;
  }

  uv_mutex_unlock(&ODBC::g_odbcMutex);
}

/*
 * CreateConnection
 */

class CreateConnectionAsyncWorker : public DeferredAsyncWorker {

  public:
    CreateConnectionAsyncWorker(ODBC *odbcObject, Napi::Promise::Deferred deferred)
      : DeferredAsyncWorker(deferred), odbcObject(odbcObject) {}

    ~CreateConnectionAsyncWorker() {}

    void Execute() {
      DEBUG_PRINTF("ODBC::CreateConnectionAsyncWorker::Execute\n");

      uv_mutex_lock(&ODBC::g_odbcMutex);
      //allocate a new connection handle
      sqlReturnCode = SQLAllocHandle(SQL_HANDLE_DBC, odbcObject->m_hEnv, &hDBC);
      uv_mutex_unlock(&ODBC::g_odbcMutex);
    }

    void OnOK() {
      DEBUG_PRINTF("ODBC::CreateConnectionAsyncWorker::OnOk\n");

      Napi::Env env = Env();
      Napi::HandleScope scope(env);

      if (!SQL_SUCCEEDED(sqlReturnCode)) {
        // Return the SQLError

        Reject(GetSQLError(env, SQL_HANDLE_ENV, odbcObject->m_hEnv));
      } else {
        // Return a connection

        // Create a new ODBCConnection instance

        // Pass the HENV and HDBC values to the ODBCConnection constructor
        std::vector<napi_value> connectionArguments;
        connectionArguments.push_back(Napi::External<SQLHENV>::New(env, &(odbcObject->m_hEnv))); // connectionArguments[0]
        connectionArguments.push_back(Napi::External<SQLHDBC>::New(env, &hDBC));   // connectionArguments[1]

        // Create a new ODBCConnection object as a Napi::Value
        Napi::Value connectionObject = ODBCConnection::constructor.New(connectionArguments);

        Resolve(connectionObject);
      }
    }

  private:
    ODBC *odbcObject;
    SQLRETURN sqlReturnCode;
    SQLHDBC hDBC;
};

Napi::Value ODBC::CreateConnection(const Napi::CallbackInfo& info) {

  DEBUG_PRINTF("ODBC::CreateConnection\n");

  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(info.Env());

  CreateConnectionAsyncWorker *worker = new CreateConnectionAsyncWorker(this, deferred);
  worker->Queue();

  return deferred.Promise();
}


#ifdef dynodbc
Napi::Value ODBC::LoadODBCLibrary(const Napi::CallbackInfo& info) {
  Napi::HandleScope scope(env);

  REQ_STR_ARG(0, js_library);

  bool result = DynLoadODBC(*js_library);

  return (result) ? env.True() : env.False();
}
#endif
