#include "utils.h"

Napi::Value EmptyCallback(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    return env.Undefined();
}

// Take a Napi::String, and convert it to an SQLTCHAR*
SQLTCHAR* NapiStringToSQLTCHAR(Napi::String string) {

  #ifdef UNICODE
    std::u16string tempString = string.Utf16Value();
  #else
    std::string tempString = string.Utf8Value();
  #endif
  std::vector<SQLTCHAR> *stringVector = new std::vector<SQLTCHAR>(tempString.begin(), tempString.end());
  stringVector->push_back('\0');
  return &(*stringVector)[0];
}

void Fetch(QueryData *data) {

  if (SQL_SUCCEEDED(SQLFetch(data->hSTMT))) {

    ColumnData *row = new ColumnData[data->columnCount];

    // Iterate over each column, putting the data in the row object
    // Don't need to use intermediate structure in sync version
    for (int i = 0; i < data->columnCount; i++) {

      row[i].size = data->columns[i].dataLength;
      if (row[i].size == SQL_NULL_DATA) {
        row[i].data = NULL;
      } else {
        row[i].data = new SQLTCHAR[row[i].size];
        memcpy(row[i].data, data->boundRow[i], row[i].size);
      }
    }

    data->storedRows.push_back(row);
  }
}

void FetchAll(QueryData *data) {
  // continue call SQLFetch, with results going in the boundRow array
  while(SQL_SUCCEEDED(SQLFetch(data->hSTMT))) {

    ColumnData *row = new ColumnData[data->columnCount];

    // Iterate over each column, putting the data in the row object
    // Don't need to use intermediate structure in sync version
    for (int i = 0; i < data->columnCount; i++) {

      row[i].size = data->columns[i].dataLength;
      if (row[i].size == SQL_NULL_DATA) {
        row[i].data = NULL;
      } else {
        row[i].data = new SQLTCHAR[row[i].size];
        memcpy(row[i].data, data->boundRow[i], row[i].size);
      }
    }

    data->storedRows.push_back(row);
  }
}

void BindColumns(QueryData *data) {

  // SQLNumResultCols returns the number of columns in a result set.
  data->sqlReturnCode = SQLNumResultCols(
                          data->hSTMT,       // StatementHandle
                          &data->columnCount // ColumnCountPtr
                        );

  // if there was an error, set columnCount to 0 and return
  // TODO: Should throw an error?
  if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
    data->columnCount = 0;
    return;
  }

  // create Columns for the column data to go into
  data->columns = new Column[data->columnCount];
  data->boundRow = new SQLTCHAR*[data->columnCount];

  for (int i = 0; i < data->columnCount; i++) {

    data->columns[i].index = i + 1; // Column number of result data, starting at 1
    data->columns[i].name = new SQLTCHAR[SQL_MAX_COLUMN_NAME_LEN]();
      data->sqlReturnCode = SQLDescribeCol(
      data->hSTMT,                   // StatementHandle
      data->columns[i].index,        // ColumnNumber
      data->columns[i].name,         // ColumnName
      SQL_MAX_COLUMN_NAME_LEN,       // BufferLength,
      &(data->columns[i].nameSize),  // NameLengthPtr,
      &(data->columns[i].type),      // DataTypePtr
      &(data->columns[i].precision), // ColumnSizePtr,
      &(data->columns[i].scale),     // DecimalDigitsPtr,
      &(data->columns[i].nullable)   // NullablePtr
    );

    // TODO: Should throw an error?
    if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
      return;
    }

    SQLLEN maxColumnLength;
    SQLSMALLINT targetType;

    // bind depending on the column
    switch(data->columns[i].type) {

      case SQL_DECIMAL :
      case SQL_NUMERIC :

        maxColumnLength = data->columns[i].precision;
        targetType = SQL_C_CHAR;
        break;

      case SQL_DOUBLE :

        maxColumnLength = data->columns[i].precision;
        targetType = SQL_C_DOUBLE;
        break;

      case SQL_INTEGER :

        printf("it is an integer, but has no presicion");
        maxColumnLength = data->columns[i].precision;
        targetType = SQL_C_SLONG;
        break;

      case SQL_BIGINT :

       maxColumnLength = data->columns[i].precision;
       targetType = SQL_C_SBIGINT;
       break;

      case SQL_BINARY :
      case SQL_VARBINARY :
      case SQL_LONGVARBINARY :

        maxColumnLength = data->columns[i].precision;
        targetType = SQL_C_BINARY;
        break;

      case SQL_WCHAR :
      case SQL_WVARCHAR :

        maxColumnLength = (data->columns[i].precision << 2) + 1;
        targetType = SQL_C_CHAR;
        break;

      default:

        //maxColumnLength = columns[i].precision + 1;
        maxColumnLength = 250;
        targetType = SQL_C_CHAR;
        break;
    }

    data->boundRow[i] = new SQLTCHAR[maxColumnLength]();

    // SQLBindCol binds application data buffers to columns in the result set.
    data->sqlReturnCode = SQLBindCol(
      data->hSTMT,              // StatementHandle
      i + 1,                    // ColumnNumber
      targetType,               // TargetType
      data->boundRow[i],        // TargetValuePtr
      maxColumnLength,          // BufferLength
      &(data->columns[i].dataLength)  // StrLen_or_Ind
    );

    // TODO: Error
    if (!SQL_SUCCEEDED(data->sqlReturnCode)) {
      return;
    }
  }
}

void BindParameters(QueryData *data) {

  for (int i = 0; i < data->paramCount; i++) {

    Parameter prm = data->params[i];

    DEBUG_TPRINTF(
      SQL_T("ODBCConnection::UV_Query - param[%i]: ValueType=%i type=%i BufferLength=%i size=%i\n"), i, prm.ValueType, prm.ParameterType,
      prm.BufferLength, prm.ColumnSize);

    data->sqlReturnCode = SQLBindParameter(
      data->hSTMT,                        // StatementHandle
      i + 1,                              // ParameterNumber
      prm.InputOutputType,                // InputOutputType
      prm.ValueType,                      // ValueType
      prm.ParameterType,                  // ParameterType
      prm.ColumnSize,                     // ColumnSize
      prm.DecimalDigits,                  // DecimalDigits
      prm.ParameterValuePtr,              // ParameterValuePtr
      prm.BufferLength,                   // BufferLength
      &data->params[i].StrLen_or_IndPtr); // StrLen_or_IndPtr

    if (data->sqlReturnCode == SQL_ERROR) {
      return;
    }
  }
}

Parameter* GetParametersFromArray(Napi::Array *values, int *paramCount) {

  DEBUG_PRINTF("GetParametersFromArray\n");

  *paramCount = values->Length();

  Parameter* params = NULL;

  if (*paramCount > 0) {
    params = new Parameter[*paramCount];
  }

  for (int i = 0; i < *paramCount; i++) {

    Napi::Value value;
    Napi::Number ioType;

    Napi::Value param = values->Get(i);

    // these are the default values, overwritten in some cases
    params[i].ColumnSize       = 0;
    params[i].StrLen_or_IndPtr = SQL_NULL_DATA;
    params[i].BufferLength     = 0;
    params[i].DecimalDigits    = 0;

    value = param;
    params[i].InputOutputType = SQL_PARAM_INPUT_OUTPUT;

    DetermineParameterType(value, &params[i]);
  }

  return params;
}

Napi::Array GetNapiRowData(Napi::Env env, std::vector<ColumnData*> *storedRows, Column *columns, int columnCount, int fetchMode) {

  printf("\nGetNapiRowData\n");

  //Napi::HandleScope scope(env);
  Napi::Array rows = Napi::Array::New(env);

  for (unsigned int i = 0; i < storedRows->size(); i++) {

    // Arrays are a subclass of Objects
    Napi::Object row;

    if (fetchMode == FETCH_ARRAY) {
      row = Napi::Array::New(env);
    } else {
      row = Napi::Object::New(env);
    }

    ColumnData *storedRow = (*storedRows)[i];

    // Iterate over each column, putting the data in the row object
    // Don't need to use intermediate structure in sync version
    for (int j = 0; j < columnCount; j++) {

      Napi::Value value;

      // check for null data
      if (storedRow[j].size == SQL_NULL_DATA) {

        value = env.Null();

      } else {

        switch(columns[j].type) {
          // Napi::Number
          case SQL_DECIMAL :
          case SQL_NUMERIC :
          case SQL_FLOAT :
          case SQL_REAL :
          case SQL_DOUBLE :
            value = Napi::Number::New(env, *(double*)storedRow[j].data);
            break;
          case SQL_INTEGER :
          case SQL_SMALLINT :
          case SQL_BIGINT :
            value = Napi::Number::New(env, *(int32_t*)storedRow[j].data);
            break;
          // Napi::ArrayBuffer
          case SQL_BINARY :
          case SQL_VARBINARY :
          case SQL_LONGVARBINARY :
            value = Napi::ArrayBuffer::New(env, storedRow[j].data, storedRow[j].size);
            break;
          // Napi::String (char16_t)
          case SQL_WCHAR :
          case SQL_WVARCHAR :
          case SQL_WLONGVARCHAR :
            value = Napi::String::New(env, (const char16_t*)storedRow[j].data, storedRow[j].size);
            break;
          // Napi::String (char)
          case SQL_CHAR :
          case SQL_VARCHAR :
          case SQL_LONGVARCHAR :
          default:
            value = Napi::String::New(env, (const char*)storedRow[j].data, storedRow[j].size);
            break;
        }
      }

      if (fetchMode == FETCH_ARRAY) {
        row.Set(j, value);
      } else {
        row.Set(Napi::String::New(env, (const char*)columns[j].name), value);
      }

      delete storedRow[j].data;
      //delete storedRow;
    }
    rows.Set(i, row);
  }

  storedRows->clear();

  return rows;
}

Napi::Object GetSQLError(Napi::Env env, SQLSMALLINT handleType, SQLHANDLE handle) {

  return GetSQLError(env, handleType, handle, "[node-odbc] SQL_ERROR");
}

Napi::Object GetSQLError(Napi::Env env, SQLSMALLINT handleType, SQLHANDLE handle, const char* message) {
  DEBUG_PRINTF("GetSQLError : handleType=%i, handle=%p\n", handleType, handle);

  Napi::Object objError = Napi::Object::New(env);

  int32_t i = 0;
  SQLINTEGER native;

  SQLSMALLINT len;
  SQLINTEGER statusRecCount;
  SQLRETURN ret;
  char errorSQLState[14];
  char errorMessage[ERROR_MESSAGE_BUFFER_BYTES];

  ret = SQLGetDiagField(
    handleType,
    handle,
    0,
    SQL_DIAG_NUMBER,
    &statusRecCount,
    SQL_IS_INTEGER,
    &len);

  // Windows seems to define SQLINTEGER as long int, unixodbc as just int... %i should cover both
  DEBUG_PRINTF("GetSQLError : called SQLGetDiagField; ret=%i, statusRecCount=%i\n", ret, statusRecCount);

  Napi::Array errors = Napi::Array::New(env);

  objError.Set(Napi::String::New(env, "errors"), errors);

  for (i = 0; i < statusRecCount; i++){

    DEBUG_PRINTF("GetSQLError : calling SQLGetDiagRec; i=%i, statusRecCount=%i\n", i, statusRecCount);

    ret = SQLGetDiagRec(
      handleType,
      handle,
      (SQLSMALLINT)(i + 1),
      (SQLTCHAR *) errorSQLState,
      &native,
      (SQLTCHAR *) errorMessage,
      ERROR_MESSAGE_BUFFER_CHARS,
      &len);

    DEBUG_PRINTF("GetSQLError : after SQLGetDiagRec; i=%i\n", i);

    if (SQL_SUCCEEDED(ret)) {
      DEBUG_PRINTF("GetSQLError : errorMessage=%s, errorSQLState=%s\n", errorMessage, errorSQLState);

      if (i == 0) {
        // First error is assumed the primary error
        objError.Set(Napi::String::New(env, "error"), Napi::String::New(env, message));
#ifdef UNICODE
        //objError.SetPrototype(Exception::Error(Napi::String::New(env, (char16_t *) errorMessage)));
        objError.Set(Napi::String::New(env, "message"), Napi::String::New(env, (char16_t *) errorMessage));
        objError.Set(Napi::String::New(env, "state"), Napi::String::New(env, (char16_t *) errorSQLState));
#else
        //objError.SetPrototype(Exception::Error(Napi::String::New(env, errorMessage)));
        objError.Set(Napi::String::New(env, "message"), Napi::String::New(env, errorMessage));
        objError.Set(Napi::String::New(env, "state"), Napi::String::New(env, errorSQLState));
#endif
      }

      Napi::Object subError = Napi::Object::New(env);

#ifdef UNICODE
      subError.Set(Napi::String::New(env, "message"), Napi::String::New(env, (char16_t *) errorMessage));
      subError.Set(Napi::String::New(env, "state"), Napi::String::New(env, (char16_t *) errorSQLState));
#else
      subError.Set(Napi::String::New(env, "message"), Napi::String::New(env, errorMessage));
      subError.Set(Napi::String::New(env, "state"), Napi::String::New(env, errorSQLState));
#endif
      errors.Set(Napi::String::New(env, std::to_string(i)), subError);

    } else if (ret == SQL_NO_DATA) {
      break;
    }
  }

  if (statusRecCount == 0) {
    //Create a default error object if there were no diag records
    objError.Set(Napi::String::New(env, "error"), Napi::String::New(env, message));
    //objError.SetPrototype(Napi::Error(Napi::String::New(env, message)));
    objError.Set(Napi::String::New(env, "message"), Napi::String::New(env,
      (const char *) "[node-odbc] An error occurred but no diagnostic information was available."));

  }

  return objError;
}

void DetermineParameterType(Napi::Value value, Parameter *param) {

  if (value.IsNull()) {

      param->ValueType = SQL_C_DEFAULT;
      param->ParameterType   = SQL_VARCHAR;
      param->StrLen_or_IndPtr = SQL_NULL_DATA;
  }
  else if (value.IsNumber()) {
    // check whether it is an INT or a Double
    double orig_val = value.As<Napi::Number>().DoubleValue();
    int64_t int_val = value.As<Napi::Number>().Int64Value();

    if (orig_val == int_val) {
      // is an integer
      int64_t  *number = new int64_t(value.As<Napi::Number>().Int64Value());
      param->ValueType = SQL_C_SBIGINT;
      param->ParameterType   = SQL_BIGINT;
      param->ParameterValuePtr = number;
      param->StrLen_or_IndPtr = 0;

      DEBUG_PRINTF("GetParametersFromArray - IsInt32(): params[%i] c_type=%i type=%i buffer_length=%lli size=%lli length=%lli value=%lld\n",
                    i, param->ValueType, param->ParameterType,
                    param->BufferLength, param->ColumnSize, param->StrLen_or_IndPtr,
                    *number);
    } else {
      // not an integer
      double *number   = new double(value.As<Napi::Number>().DoubleValue());

      param->ValueType         = SQL_C_DOUBLE;
      param->ParameterType     = SQL_DOUBLE;
      param->ParameterValuePtr = number;
      param->BufferLength      = sizeof(double);
      param->StrLen_or_IndPtr  = param->BufferLength;
      param->DecimalDigits     = 7;
      param->ColumnSize        = sizeof(double);

      DEBUG_PRINTF("GetParametersFromArray - IsNumber(): params[%i] c_type=%i type=%i buffer_length=%lli size=%lli length=%lli value=%f\n",
                    i, param->ValueType, param->ParameterType,
                    param->BufferLength, param->ColumnSize, param->StrLen_or_IndPtr,
                    *number);
    }
  }
  else if (value.IsBoolean()) {
    bool *boolean = new bool(value.As<Napi::Boolean>().Value());
    param->ValueType         = SQL_C_BIT;
    param->ParameterType     = SQL_BIT;
    param->ParameterValuePtr = boolean;
    param->StrLen_or_IndPtr  = 0;

    DEBUG_PRINTF("GetParametersFromArray - IsBoolean(): params[%i] c_type=%i type=%i buffer_length=%lli size=%lli length=%lli\n",
                  i, param->ValueType, param->ParameterType,
                  param->BufferLength, param->ColumnSize, param->StrLen_or_IndPtr);
  }
  else { // Default to string

    Napi::String string = value.ToString();

    param->ValueType         = SQL_C_TCHAR;
    param->ColumnSize        = 0; //SQL_SS_LENGTH_UNLIMITED
    #ifdef UNICODE
          param->ParameterType     = SQL_WVARCHAR;
          param->BufferLength      = (string.Utf16Value().length() * sizeof(char16_t)) + sizeof(char16_t);
    #else
          param->ParameterType     = SQL_VARCHAR;
          param->BufferLength      = string.Utf8Value().length() + 1;
    #endif
          param->ParameterValuePtr = malloc(param->BufferLength);
          param->StrLen_or_IndPtr  = SQL_NTS; //param->BufferLength;

    #ifdef UNICODE
          memcpy((char16_t *) param->ParameterValuePtr, string.Utf16Value().c_str(), param->BufferLength);
    #else
          memcpy((char *) param->ParameterValuePtr, string.Utf8Value().c_str(), param->BufferLength);
    #endif

    DEBUG_PRINTF("GetParametersFromArray - IsString(): params[%i] c_type=%i type=%i buffer_length=%lli size=%lli length=%lli value=%s\n",
                  i, param->ValueType, param->ParameterType,
                  param->BufferLength, param->ColumnSize, param->StrLen_or_IndPtr,
                  (char*) param->ParameterValuePtr);
  }
}
