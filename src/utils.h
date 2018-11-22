#ifndef _SRC_UTILS_H
#define _SRC_UTILS_H

#include "declarations.h"

 // Look at https://github.com/nodejs/abi-stable-node-addon-examples/issues/23
Napi::Value EmptyCallback(const Napi::CallbackInfo& info);

SQLTCHAR* NapiStringToSQLTCHAR(Napi::String string);

void FetchData(QueryData *data);

void FetchAllData(QueryData *data);

void BindColumns(QueryData *data);

void BindParameters(QueryData *data);

Parameter* GetParametersFromArray(Napi::Array *values, int *paramCount);

Napi::Array GetNapiRowData(Napi::Env env, std::vector<ColumnData*> *storedRows, Column *columns, int columnCount, int fetchMode);

Napi::Object GetSQLError(Napi::Env env, SQLSMALLINT handleType, SQLHANDLE handle);

Napi::Object GetSQLError(Napi::Env env, SQLSMALLINT handleType, SQLHANDLE handle, const char* message);

void DetermineParameterType(Napi::Value value, Parameter *param);

#endif
