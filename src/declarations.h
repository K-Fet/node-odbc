#include <napi.h>
#include <wchar.h>
#include <stdlib.h>
#include <uv.h>

#ifdef dynodbc
#include "dynodbc.h"
#else
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <sqlucode.h>
#endif

#define MAX_FIELD_SIZE 1024
#define MAX_VALUE_SIZE 1048576

#ifdef UNICODE
#define ERROR_MESSAGE_BUFFER_BYTES 2048
#define ERROR_MESSAGE_BUFFER_CHARS 1024
#else
#define ERROR_MESSAGE_BUFFER_BYTES 2048
#define ERROR_MESSAGE_BUFFER_CHARS 2048
#endif

#define MODE_COLLECT_AND_CALLBACK 1
#define MODE_CALLBACK_FOR_EACH 2
#define FETCH_ARRAY 3
#define FETCH_OBJECT 4
#define SQL_DESTROY 9999

typedef struct Column {
  SQLUSMALLINT  index;
  SQLTCHAR      *name;
  SQLSMALLINT   nameSize;
  SQLSMALLINT   type;
  SQLULEN       precision;
  SQLSMALLINT   scale;
  SQLSMALLINT   nullable;
  SQLLEN        dataLength;
} Column;

typedef struct Parameter {
  SQLSMALLINT  InputOutputType;
  SQLSMALLINT  ValueType;
  SQLSMALLINT  ParameterType;
  SQLLEN       ColumnSize;
  SQLSMALLINT  DecimalDigits;
  void        *ParameterValuePtr;
  SQLLEN       BufferLength;
  SQLLEN       StrLen_or_IndPtr;
} Parameter;

typedef struct ColumnData {
  SQLTCHAR *data;
  int      size;
} ColumnData;

typedef struct QueryData {

  HSTMT hSTMT;

  int fetchMode;
  bool noResultObject = false;

  Napi::Value objError;

  // parameters
  Parameter *params;
  int paramCount = 0;
  int completionType;

  // columns and rows
  Column                    *columns;
  SQLSMALLINT                columnCount;
  SQLTCHAR                 **boundRow;
  std::vector<ColumnData*>   storedRows;

  // query options
  bool useCursor = false;
  int fetchCount = 0;

  SQLTCHAR *sql     = NULL;
  SQLTCHAR *catalog = NULL;
  SQLTCHAR *schema  = NULL;
  SQLTCHAR *table   = NULL;
  SQLTCHAR *type    = NULL;
  SQLTCHAR *column  = NULL;

  SQLRETURN sqlReturnCode;

  ~QueryData() {

    if (this->paramCount) {
      Parameter prm;
      // free parameters
      for (int i = 0; i < this->paramCount; i++) {
        if (prm = this->params[i], prm.ParameterValuePtr != NULL) {
          switch (prm.ValueType) {
            case SQL_C_WCHAR:   free(prm.ParameterValuePtr);             break;
            case SQL_C_CHAR:    free(prm.ParameterValuePtr);             break;
            case SQL_C_LONG:    delete (int64_t *)prm.ParameterValuePtr; break;
            case SQL_C_DOUBLE:  delete (double  *)prm.ParameterValuePtr; break;
            case SQL_C_BIT:     delete (bool    *)prm.ParameterValuePtr; break;
          }
        }
      }

      free(this->params);
    }

    delete columns;
    delete boundRow;

    free(this->sql);
    free(this->catalog);
    free(this->schema);
    free(this->table);
    free(this->type);
    free(this->column);
  }

} QueryData;

#ifdef UNICODE
#define SQL_T(x) (L##x)
#else
#define SQL_T(x) (x)
#endif

#ifdef DEBUG
#define DEBUG_TPRINTF(...) fprintf(stdout, __VA_ARGS__)
#define DEBUG_PRINTF(...) fprintf(stdout, __VA_ARGS__)
#else
#define DEBUG_PRINTF(...) (void)0
#define DEBUG_TPRINTF(...) (void)0
#endif

#define REQ_ARGS(N)                                                     \
  if (info.Length() < (N))                                              \
    Napi::TypeError::New(env, "Expected " #N "arguments").ThrowAsJavaScriptException(); \
    return env.Null();

//Require String Argument; Save String as Utf8
#define REQ_STR_ARG(I, VAR)                                             \
  if (info.Length() <= (I) || !info[I].IsString())                     \
    Napi::TypeError::New(env, "Argument " #I " must be a string").ThrowAsJavaScriptException(); \
    return env.Null();       \
  Napi::String VAR(env, info[I].ToString());

//Require String Argument; Save String as Wide String (UCS2)
#define REQ_WSTR_ARG(I, VAR)                                            \
  if (info.Length() <= (I) || !info[I].IsString())                     \
    Napi::TypeError::New(env, "Argument " #I " must be a string").ThrowAsJavaScriptException(); \
    return env.Null();       \
  String::Value VAR(info[I].ToString());

//Require String Argument; Save String as Object
#define REQ_STRO_ARG(I, VAR)                                            \
  if (info.Length() <= (I) || !info[I].IsString())                     \
    Napi::TypeError::New(env, "Argument " #I " must be a string").ThrowAsJavaScriptException(); \
    return env.Null();       \
  Napi::String VAR(info[I].ToString());

//Require String or Null Argument; Save String as Utf8
#define REQ_STR_OR_NULL_ARG(I, VAR)                                             \
  if ( info.Length() <= (I) || (!info[I].IsString() && !info[I].IsNull()) )   \
    Napi::TypeError::New(env, "Argument " #I " must be a string or null").ThrowAsJavaScriptException(); \
    return env.Null();       \
  Napi::String VAR(env, info[I].ToString());

//Require String or Null Argument; Save String as Wide String (UCS2)
#define REQ_WSTR_OR_NULL_ARG(I, VAR)                                              \
  if ( info.Length() <= (I) || (!info[I].IsString() && !info[I].IsNull()) )     \
    Napi::TypeError::New(env, "Argument " #I " must be a string or null").ThrowAsJavaScriptException(); \
    return env.Null();         \
  String::Value VAR(info[I].ToString());

//Require String or Null Argument; save String as String Object
#define REQ_STRO_OR_NULL_ARG(I, VAR)                                              \
  if ( info.Length() <= (I) || (!info[I].IsString() && !info[I].IsNull()) ) {   \
    Napi::TypeError::New(env, "Argument " #I " must be a string or null").ThrowAsJavaScriptException(); \
                \
    return env.Null();                                                         \
  }                                                                               \
  Napi::String VAR(info[I].ToString());

#define REQ_FUN_ARG(I, VAR)                                             \
  if (info.Length() <= (I) || !info[I].IsFunction())                   \
    Napi::TypeError::New(env, "Argument " #I " must be a function").ThrowAsJavaScriptException(); \
    return env.Null();     \
  Napi::Function VAR = info[I].As<Napi::Function>();

#define REQ_BOOL_ARG(I, VAR)                                            \
  if (info.Length() <= (I) || !info[I].IsBoolean())                    \
    Napi::TypeError::New(env, "Argument " #I " must be a boolean").ThrowAsJavaScriptException(); \
    return env.Null();      \
  Napi::Boolean VAR = (info[I].ToBoolean());

#define REQ_EXT_ARG(I, VAR, TYPE)                                             \
  if (info.Length() <= (I) || !info[I].IsExternal())                   \
    Napi::TypeError::New(env, "Argument " #I " invalid").ThrowAsJavaScriptException(); \
    return env.Null();                \
  Napi::External<TYPE> VAR = info[I].As<Napi::External<TYPE>>();

#define OPT_INT_ARG(I, VAR, DEFAULT)                                    \
  int VAR;                                                              \
  if (info.Length() <= (I)) {                                           \
    VAR = (DEFAULT);                                                    \
          } else if (info[I].IsNumber()) {                                      \
    VAR = info[I].As<Napi::Number>().Int32Value();                                        \
  } else {                                                              \
    Napi::TypeError::New(env, "Argument " #I " must be an integer").ThrowAsJavaScriptException(); \
    return env.Null();     \
  }


// From node v10 NODE_DEFINE_CONSTANT
#define NODE_ODBC_DEFINE_CONSTANT(constructor, constant)       \
  (constructor).Set(Napi::String::New(env, #constant),                \
                Napi::Number::New(env, constant),                               \
                static_cast<v8::PropertyAttribute>(v8::ReadOnly|v8::DontDelete))
