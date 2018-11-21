#include <napi.h>
#include "odbc.h"
#include "odbc_connection.h"
#include "odbc_result.h"
#include "odbc_statement.h"


Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ODBC::Init(env, exports);
  ODBCConnection::Init(env, exports);
  ODBCStatement::Init(env, exports);
  ODBCResult::Init(env, exports);

  // adding constant properties to the
  std::vector<Napi::PropertyDescriptor> ODBC_VALUES;

  // // type values
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_CHAR", Napi::Number::New(env, SQL_CHAR)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_VARCHAR", Napi::Number::New(env, SQL_VARCHAR)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_LONGVARCHAR", Napi::Number::New(env, SQL_LONGVARCHAR)));

  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_BIGINT", Napi::Number::New(env, SQL_BIGINT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_BIT", Napi::Number::New(env, SQL_BIT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_INTEGER", Napi::Number::New(env, SQL_INTEGER)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_NUMERIC", Napi::Number::New(env, SQL_NUMERIC)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_SMALLINT", Napi::Number::New(env, SQL_SMALLINT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_TINYINT", Napi::Number::New(env, SQL_TINYINT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_BIT", Napi::Number::New(env, SQL_BIT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_INTEGER", Napi::Number::New(env, SQL_INTEGER)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_DECIMAL", Napi::Number::New(env, SQL_DECIMAL)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_DOUBLE", Napi::Number::New(env, SQL_DOUBLE)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_FLOAT", Napi::Number::New(env, SQL_FLOAT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_BINARY", Napi::Number::New(env, SQL_BINARY)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_VARBINARY", Napi::Number::New(env, SQL_VARBINARY)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_LONGVARBINARY", Napi::Number::New(env, SQL_LONGVARBINARY)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_TYPE_DATE", Napi::Number::New(env, SQL_TYPE_DATE)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_TYPE_TIME", Napi::Number::New(env, SQL_TYPE_TIME)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_TYPE_TIMESTAMP", Napi::Number::New(env, SQL_TYPE_TIMESTAMP)));

  // // binding values
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_PARAM_INPUT", Napi::Number::New(env, SQL_PARAM_INPUT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_PARAM_INPUT_OUTPUT", Napi::Number::New(env, SQL_PARAM_INPUT_OUTPUT)));
  // ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_PARAM_OUTPUT", Napi::Number::New(env, SQL_PARAM_OUTPUT)));

  // fetch_array
  ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("FETCH_ARRAY", Napi::Number::New(env, FETCH_ARRAY)));
  ODBC_VALUES.push_back(Napi::PropertyDescriptor::Value("SQL_USER_NAME", Napi::Number::New(env, SQL_USER_NAME)));

  exports.DefineProperties(ODBC_VALUES);

#ifdef dynodbc
    exports.Set(Napi::String::New(env, "loadODBCLibrary"), Napi::Function::New(env, ODBC::LoadODBCLibrary);());
#endif

  return exports;
}

NODE_API_MODULE(NODE_GYP_MODULE_NAME, InitAll)
