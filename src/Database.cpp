/*
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

#include <string.h>
#include <v8.h>
#include <node.h>
#include <node_version.h>
#include <time.h>
#include <uv.h>

#include "Database.h"
#ifdef _WIN32
#include "strptime.h"
#endif


#define MAX_FIELD_SIZE 1024
#define MAX_VALUE_SIZE 1048576

using namespace v8;
using namespace node;

uv_mutex_t Database::g_odbcMutex;
uv_async_t Database::g_async;

void Database::Init(v8::Handle<Object> target) {
  HandleScope scope;

  Local<FunctionTemplate> t = FunctionTemplate::New(New);

  // Constructor Template
  constructor_template = Persistent<FunctionTemplate>::New(t);
  constructor_template->SetClassName(String::NewSymbol("Database"));

  // Reserve space for one Handle<Value>
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  
  // Prototype Methods
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "dispatchOpen", Open);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "dispatchClose", Close);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "dispatchQuery", Query);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "dispatchTables", Tables);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "dispatchColumns", Columns);

  // Attach the Database Constructor to the target object
  target->Set(v8::String::NewSymbol("Database"), constructor_template->GetFunction());
  
  scope.Close(Undefined());
  
  // Initialize uv_async so that we can prevent node from exiting
  uv_async_init(uv_default_loop(), &Database::g_async, Database::WatcherCallback);
  
  // Initialize the cross platform mutex provided by libuv
  uv_mutex_init(&Database::g_odbcMutex);
}

Handle<Value> Database::New(const Arguments& args) {
  HandleScope scope;
  Database* dbo = new Database();
  dbo->Wrap(args.This());
  
  return scope.Close(args.This());
}

void Database::WatcherCallback(uv_async_t *w, int revents) {
  //i don't know if we need to do anything here
}

void Database::UV_AfterOpen(uv_work_t* req) {
  HandleScope scope;
  open_request* open_req = (open_request *)(req->data);

  Local<Value> argv[1];
  
  bool err = false;

  if (open_req->result) {
    err = true;

    SQLINTEGER i = 0;
    SQLINTEGER native;
    SQLSMALLINT len;
    SQLRETURN ret;
    char errorSQLState[7];
    char errorMessage[256];

    do {
      ret = SQLGetDiagRec( SQL_HANDLE_DBC, 
                           open_req->dbo->self()->m_hDBC,
                           ++i, 
                           (SQLCHAR *) errorSQLState,
                           &native,
                           (SQLCHAR *) errorMessage,
                           sizeof(errorMessage),
                           &len );

      if (SQL_SUCCEEDED(ret)) {
        Local<Object> objError = Object::New();

        objError->Set(String::New("error"), String::New("[node-odbc] SQL_ERROR"));
        objError->Set(String::New("message"), String::New(errorMessage));
        objError->Set(String::New("state"), String::New(errorSQLState));

        argv[0] = objError;
      }
    } while( ret == SQL_SUCCESS );
  }

  TryCatch try_catch;

  open_req->dbo->Unref();
  open_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  open_req->cb.Dispose();

#if NODE_VERSION_AT_LEAST(0, 7, 9)
  uv_ref((uv_handle_t *)&Database::g_async);
#else
  uv_ref(uv_default_loop());
#endif
  
  free(open_req);
  free(req);
  scope.Close(Undefined());
}

void Database::UV_Open(uv_work_t* req) {
  open_request* open_req = (open_request *)(req->data);
  Database* self = open_req->dbo->self();
  
  uv_mutex_lock(&Database::g_odbcMutex);
  
  int ret = SQLAllocEnv( &self->m_hEnv );

  if( ret == SQL_SUCCESS ) {
    ret = SQLAllocConnect( self->m_hEnv,&self->m_hDBC );

    if( ret == SQL_SUCCESS ) {
      SQLSetConnectOption( self->m_hDBC, SQL_LOGIN_TIMEOUT, 5 );

      char connstr[1024];

      //Attempt to connect
      ret = SQLDriverConnect( self->m_hDBC, 
                              NULL,
                              (SQLCHAR*) open_req->connection,
                              strlen(open_req->connection),
                              (SQLCHAR*) connstr,
                              1024,
                              NULL,
                              SQL_DRIVER_NOPROMPT);

      if( ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO ) {
        ret = SQLAllocStmt( self->m_hDBC, &self->m_hStmt );

        if (ret != SQL_SUCCESS) printf("not connected\n");

        ret = SQLGetFunctions( self->m_hDBC,
                               SQL_API_SQLMORERESULTS, 
                               &self->canHaveMoreResults);

        if ( !SQL_SUCCEEDED(ret)) {
          self->canHaveMoreResults = 0;
        }
      }
    }
  }
  
  uv_mutex_unlock(&Database::g_odbcMutex);
  open_req->result = ret;
}

Handle<Value> Database::Open(const Arguments& args) {
  HandleScope scope;

  REQ_STR_ARG(0, connection);
  REQ_FUN_ARG(1, cb);

  Database* dbo = ObjectWrap::Unwrap<Database>(args.This());
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  open_request* open_req = (open_request *) calloc(1, sizeof(open_request) + connection.length());

  if (!open_req) {
    V8::LowMemoryNotification();
    return ThrowException(Exception::Error(String::New("Could not allocate enough memory")));
  }

  strcpy(open_req->connection, *connection);
  open_req->cb = Persistent<Function>::New(cb);
  open_req->dbo = dbo;
  
  work_req->data = open_req;
  
  uv_queue_work(uv_default_loop(), work_req, UV_Open, (uv_after_work_cb) UV_AfterOpen);

  dbo->Ref();

  return scope.Close(Undefined());
}

void Database::UV_AfterClose(uv_work_t* req) {
  HandleScope scope;

  close_request* close_req = (close_request *)(req->data);

  Local<Value> argv[1];
  bool err = false;
  if (close_req->result) {
    err = true;
    argv[0] = Exception::Error(String::New("Error closing database"));
  }

  TryCatch try_catch;

  close_req->dbo->Unref();
  close_req->cb->Call(Context::GetCurrent()->Global(), err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  close_req->cb.Dispose();

#if NODE_VERSION_AT_LEAST(0, 7, 9)
  uv_unref((uv_handle_t *)&Database::g_async);
#else
  uv_unref(uv_default_loop());
#endif
  
  free(close_req);
  free(req);
  scope.Close(Undefined());
}

void Database::UV_Close(uv_work_t* req) {
  close_request* close_req = (close_request *)(req->data);
  Database* dbo = close_req->dbo;
  
  uv_mutex_lock(&Database::g_odbcMutex);
  
  SQLDisconnect(dbo->m_hDBC);
  SQLFreeHandle(SQL_HANDLE_ENV, dbo->m_hEnv);
  SQLFreeHandle(SQL_HANDLE_DBC, dbo->m_hDBC);
  
  uv_mutex_unlock(&Database::g_odbcMutex);
}

Handle<Value> Database::Close(const Arguments& args) {
  HandleScope scope;

  REQ_FUN_ARG(0, cb);
//allocate a buffer for incoming column values
  Database* dbo = ObjectWrap::Unwrap<Database>(args.This());
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  close_request* close_req = (close_request *) (calloc(1, sizeof(close_request)));

  if (!close_req) {
    V8::LowMemoryNotification();
    return ThrowException(Exception::Error(String::New("Could not allocate enough memory")));
  }

  close_req->cb = Persistent<Function>::New(cb);
  close_req->dbo = dbo;

  work_req->data = close_req;
  
  uv_queue_work(uv_default_loop(), work_req, UV_Close, (uv_after_work_cb) UV_AfterClose);

  dbo->Ref();

  return scope.Close(Undefined());
}
//allocate a buffer for incoming column values
void Database::UV_AfterQuery(uv_work_t* req) {
  query_request* prep_req = (query_request *)(req->data);
  struct tm timeInfo = { 0 }; //used for processing date/time datatypes
  
  HandleScope scope;
  
  //an easy reference to the Database object
  Database* self = prep_req->dbo->self();

  //our error object which we will use if we discover errors while processing the result set
  Local<Object> objError;
  
  //used to keep track of the number of columns received in a result set
  short colCount = 0;
  
  //used to keep track of the number of event emittions that have occurred
  short emitCount = 0;
  
  //used to keep track of the number of errors that have been found
  short errorCount = 0;
  
  //used as a place holder for the length of column names
  SQLSMALLINT buflen;
  
  //used to capture the return value from various SQL function calls
  SQLRETURN ret;
  
  //allocate a buffer for incoming column values
  char* buf = (char *) malloc(MAX_VALUE_SIZE);
  
  //check to make sure malloc succeeded
  if (buf == NULL) {
    objError = Object::New();

    //malloc failed, set an error message
    objError->Set(String::New("error"), String::New("[node-odbc] Failed Malloc"));
    objError->Set(String::New("message"), String::New("An attempt to allocate memory failed. This allocation was for a value buffer of incoming recordset values."));
    
    //emit an error event immidiately.
    Local<Value> args[3];
    args[0] = objError;
    args[1] = Local<Value>::New(Null());
    args[2] = Local<Boolean>::New(False());
    
    //emit an error event
    prep_req->cb->Call(Context::GetCurrent()->Global(), 3, args);
    
    //emit a result event
    goto cleanupshutdown;
  }
  //else {
    //malloc succeeded so let's continue 
    
    //set the first byte of the buffer to \0 instead of memsetting the entire buffer to 0
    buf[0] = '\0'; 
    
    //First thing, let's check if the execution of the query returned any errors (in UV_Query)
    if(prep_req->result == SQL_ERROR) {
      objError = Object::New();

      errorCount++;
      
      char errorMessage[512];
      char errorSQLState[128];
      SQLError(self->m_hEnv, self->m_hDBC, self->m_hStmt,(SQLCHAR *)errorSQLState,NULL,(SQLCHAR *)errorMessage, sizeof(errorMessage), NULL);
      objError->Set(String::New("state"), String::New(errorSQLState));
      objError->Set(String::New("error"), String::New("[node-odbc] SQL_ERROR"));
      objError->Set(String::New("message"), String::New(errorMessage));
      
      //only set the query value of the object if we actually have a query
      if (prep_req->sql != NULL) {
        objError->Set(String::New("query"), String::New(prep_req->sql));
      }
      
      //emit an error event immidiately.
      Local<Value> args[1];
      args[0] = objError;
      prep_req->cb->Call(Context::GetCurrent()->Global(), 1, args);
      //self->Emit(String::New("error"), 1, args);
      goto cleanupshutdown;
    }
    
    //loop through all result sets
    do {
      colCount = 0; //always reset colCount for the current result set to 0;
      
      SQLNumResultCols(self->m_hStmt, &colCount);
      Column *columns = new Column[colCount];
      
      Local<Array> rows = Array::New();
      
      if (colCount > 0) {
        // retrieve and store column attributes to build the row object
        for(int i = 0; i < colCount; i++)
        {
          columns[i].name = new unsigned char[MAX_FIELD_SIZE];
          
          //set the first byte of name to \0 instead of memsetting the entire buffer
          columns[i].name[0] = '\n';
          
          //get the column name
          ret = SQLColAttribute(self->m_hStmt, (SQLUSMALLINT)i+1, SQL_DESC_LABEL, columns[i].name, (SQLSMALLINT)MAX_FIELD_SIZE, (SQLSMALLINT *)&buflen, NULL);
          
          //store the len attribute
          columns[i].len = buflen;
          
          //get the column type and store it directly in column[i].type
          ret = SQLColAttribute( self->m_hStmt, (SQLUSMALLINT)i+1, SQL_COLUMN_TYPE, NULL, 0, NULL, &columns[i].type );
        }
        
        int count = 0;
        
        // i dont think odbc will tell how many rows are returned, loop until out...
        while(true)
        {
          Local<Object> tuple = Object::New();
          ret = SQLFetch(self->m_hStmt);
          
          //TODO: Do something to enable/disable dumping these info messages to the console.
          if (ret == SQL_SUCCESS_WITH_INFO ) {
            char errorMessage[512];
            char errorSQLState[128];
            SQLError(self->m_hEnv, self->m_hDBC, self->m_hStmt,(SQLCHAR *)errorSQLState,NULL,(SQLCHAR *)errorMessage, sizeof(errorMessage), NULL);
            
            //printf("UV_Query ret => %i\n", ret);
            printf("UV_Query => %s\n", errorMessage);
            printf("UV_Query => %s\n", errorSQLState);
            //printf("UV_Query sql => %s\n", prep_req->sql);
          }

          if (ret == SQL_ERROR)  {
            objError = Object::New();

            char errorMessage[512];
            char errorSQLState[128];
            SQLError(self->m_hEnv, self->m_hDBC, self->m_hStmt,(SQLCHAR *)errorSQLState,NULL,(SQLCHAR *)errorMessage, sizeof(errorMessage), NULL);
            
            errorCount++;
            objError->Set(String::New("state"), String::New(errorSQLState));
            objError->Set(String::New("error"), String::New("[node-odbc] SQL_ERROR"));
            objError->Set(String::New("message"), String::New(errorMessage));
            objError->Set(String::New("query"), String::New(prep_req->sql));
            
            //emit an error event immidiately.
            Local<Value> args[1];
            args[0] = objError;
            prep_req->cb->Call(Context::GetCurrent()->Global(), 1, args);
            
            break;
          }
          
          if (ret == SQL_NO_DATA) {
            break;
          }
          
          for(int i = 0; i < colCount; i++)
          {
            SQLLEN len;
            
            // SQLGetData can supposedly return multiple chunks, need to do this to retrieve large fields
            int ret = SQLGetData(self->m_hStmt, i+1, SQL_CHAR, (char *) buf, MAX_VALUE_SIZE-1, (SQLLEN *) &len);
            
            //printf("%s %i\n", columns[i].name, columns[i].type);
            
            if(ret == SQL_NULL_DATA || len < 0)
            {
              tuple->Set(String::New((const char *)columns[i].name), Null());
            }
            else
            {
              switch (columns[i].type) {
                case SQL_NUMERIC :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_DECIMAL :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_INTEGER :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_SMALLINT :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_BIGINT :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_FLOAT :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_REAL :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_DOUBLE :
                  tuple->Set(String::New((const char *)columns[i].name), Number::New(atof(buf)));
                  break;
                case SQL_DATETIME :
                case SQL_TIMESTAMP :
                  //I am not sure if this is locale-safe or cross database safe, but it works for me on MSSQL
                  strptime(buf, "%Y-%m-%d %H:%M:%S", &timeInfo);
                  timeInfo.tm_isdst = -1; //a negative value means that mktime() should (use timezone information and system 
                        //databases to) attempt to determine whether DST is in effect at the specified time.
                  tuple->Set(String::New((const char *)columns[i].name), Date::New(double(mktime(&timeInfo)) * 1000));
                  
                  break;
                case SQL_BIT :
                  //again, i'm not sure if this is cross database safe, but it works for MSSQL
                  tuple->Set(String::New((const char *)columns[i].name), Boolean::New( ( *buf == '0') ? false : true ));
                  break;
                default :
                  tuple->Set(String::New((const char *)columns[i].name), String::New(buf));
                  break;
              }
            }
          }
          
          rows->Set(Integer::New(count), tuple);
          count++;
        }
        
        for(int i = 0; i < colCount; i++)
        {
          delete [] columns[i].name;
        }

        delete [] columns;
      }
      
      //move to the next result set
      ret = SQLMoreResults( self->m_hStmt );
      
      //Only trigger an emit if there are columns OR if this is the last result and none others have been emitted
      //odbc will process individual statments like select @something = 1 as a recordset even though it doesn't have
      //any columns. We don't want to emit those unless there are actually columns
      if (colCount > 0 || ( ret != SQL_SUCCESS && emitCount == 0 )) {
        emitCount++;
        
        Local<Value> args[3];
        
        if (errorCount) {
          args[0] = objError; //(objError->IsUndefined()) ? Undefined() : ;
        }
        else {
          args[0] = Local<Value>::New(Null());
        }
        
        args[1] = rows;
        args[2] = Local<Boolean>::New(( ret == SQL_SUCCESS ) ? True() : False() ); //true or false, are there more result sets to follow this emit?
        
        prep_req->cb->Call(Context::GetCurrent()->Global(), 3, args);
      }
    }
    while ( self->canHaveMoreResults && ret == SQL_SUCCESS );
  //} //end of malloc check
cleanupshutdown:
  TryCatch try_catch;
  
  self->Unref();
  
  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }
  
  free(buf);
  prep_req->cb.Dispose();
  free(prep_req->sql);
  free(prep_req->catalog);
  free(prep_req->schema);
  free(prep_req->table);
  free(prep_req->type);
  free(prep_req);
  free(req);
  scope.Close(Undefined());
}

void Database::UV_Query(uv_work_t* req) {
  query_request* prep_req = (query_request *)(req->data);
  
  Parameter prm;
  SQLRETURN ret;
  
  if(prep_req->dbo->m_hStmt)
  {
    uv_mutex_lock(&Database::g_odbcMutex);

    SQLFreeHandle( SQL_HANDLE_STMT, prep_req->dbo->m_hStmt );
    SQLAllocHandle( SQL_HANDLE_STMT, prep_req->dbo->m_hDBC, &prep_req->dbo->m_hStmt );

    uv_mutex_unlock(&Database::g_odbcMutex);
  } 

  //check to see if should excute a direct or a parameter bound query
  if (!prep_req->paramCount)
  {
    // execute the query directly
    ret = SQLExecDirect( prep_req->dbo->m_hStmt,(SQLCHAR *)prep_req->sql, strlen(prep_req->sql) );
  }
  else 
  {
    // prepare statement, bind parameters and execute statement 
    ret = SQLPrepare(prep_req->dbo->m_hStmt, (SQLCHAR *)prep_req->sql, strlen(prep_req->sql));
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
    {
      for (int i = 0; i < prep_req->paramCount; i++)
      {
        prm = prep_req->params[i];
        
        ret = SQLBindParameter(prep_req->dbo->m_hStmt, i + 1, SQL_PARAM_INPUT, prm.c_type, prm.type, prm.size, 0, prm.buffer, prm.buffer_length, &prm.length);
        if (ret == SQL_ERROR) {break;}
      }

      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        ret = SQLExecute(prep_req->dbo->m_hStmt);
      }
    }
    
    // free parameters
    //
    for (int i = 0; i < prep_req->paramCount; i++)
    {
      if (prm = prep_req->params[i], prm.buffer != NULL)
      {
        switch (prm.c_type)
        {
          case SQL_C_CHAR:    free(prm.buffer);             break; 
          case SQL_C_LONG:    delete (int64_t *)prm.buffer; break;
          case SQL_C_DOUBLE:  delete (double  *)prm.buffer; break;
          case SQL_C_BIT:     delete (bool    *)prm.buffer; break;
        }
      }
    }
    free(prep_req->params);
  }

  prep_req->result = ret; // this will be checked later in UV_AfterQuery
}

Handle<Value> Database::Query(const Arguments& args) {
  HandleScope scope;

  REQ_STR_ARG(0, sql);
  
  Local<Function> cb; 
  
  int paramCount = 0;
  Parameter* params;

  Database* dbo = ObjectWrap::Unwrap<Database>(args.This());
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  query_request* prep_req = (query_request *) calloc(1, sizeof(query_request));

  if (!prep_req) {
    V8::LowMemoryNotification();
    return ThrowException(Exception::Error(String::New("Could not allocate enough memory")));
  }

  // populate prep_req->params if parameters were supplied
  //
  if (args.Length() > 2) 
  {
      if ( !args[1]->IsArray() )
      {
           return ThrowException(Exception::TypeError(
                      String::New("Argument 1 must be an Array"))
           );
      }
      else if ( !args[2]->IsFunction() )
      {
           return ThrowException(Exception::TypeError(
                      String::New("Argument 2 must be a Function"))
           );
      }
  

      Local<Array> values = Local<Array>::Cast(args[1]);
      cb = Local<Function>::Cast(args[2]);

      prep_req->paramCount = paramCount = values->Length();
      prep_req->params     = params     = new Parameter[paramCount];

      for (int i = 0; i < paramCount; i++)
      {
          Local<Value> value = values->Get(i);

          params[i].size          = 0;
          params[i].length        = SQL_NULL_DATA;
          params[i].buffer_length = 0;

          if (value->IsString()) 
          {
              String::Utf8Value string(value);
              
              params[i].c_type        = SQL_C_CHAR;
              params[i].type          = SQL_VARCHAR;
              params[i].length        = SQL_NTS;
              params[i].buffer        = malloc(string.length() + 1);
              params[i].buffer_length = string.length() + 1;
              params[i].size          = string.length() + 1;

              strcpy((char*)params[i].buffer, *string);
          }
          else if (value->IsNull()) 
          {
              params[i].c_type = SQL_C_DEFAULT;
              params[i].type   = SQL_NULL_DATA;
              params[i].length = SQL_NULL_DATA;
          }
          else if (value->IsInt32()) 
          {
              int64_t  *number = new int64_t(value->IntegerValue());
              params[i].c_type = SQL_C_LONG;
              params[i].type   = SQL_INTEGER;
              params[i].buffer = number; 
          }
          else if (value->IsNumber()) 
          {
              double   *number = new double(value->NumberValue());
              params[i].c_type = SQL_C_DOUBLE;
              params[i].type   = SQL_DECIMAL;
              params[i].buffer = number; 
          }
          else if (value->IsBoolean()) 
          {
              bool *boolean    = new bool(value->BooleanValue());
              params[i].c_type = SQL_C_BIT;
              params[i].type   = SQL_BIT;
              params[i].buffer = boolean;
          }
      }
  }
  else 
  {
      if ( !args[1]->IsFunction() )
      {
           return ThrowException(Exception::TypeError(
                      String::New("Argument 1 must be a Function"))
           );
      }

      cb = Local<Function>::Cast(args[1]);

      prep_req->paramCount = 0;
  }

  prep_req->sql = (char *) malloc(sql.length() +1);
  prep_req->catalog = NULL;
  prep_req->schema = NULL;
  prep_req->table = NULL;
  prep_req->type = NULL;
  prep_req->column = NULL;
  prep_req->cb = Persistent<Function>::New(cb);
  
  strcpy(prep_req->sql, *sql);
  
  prep_req->dbo = dbo;
  work_req->data = prep_req;
  
  uv_queue_work(uv_default_loop(), work_req, UV_Query, (uv_after_work_cb) UV_AfterQuery);

  dbo->Ref();

  return  scope.Close(Undefined());
}

void Database::UV_Tables(uv_work_t* req) {
  query_request* prep_req = (query_request *)(req->data);
  
  if(prep_req->dbo->m_hStmt)
  {
    uv_mutex_lock(&Database::g_odbcMutex);
    SQLFreeHandle( SQL_HANDLE_STMT, prep_req->dbo->m_hStmt );
    SQLAllocStmt(prep_req->dbo->m_hDBC,&prep_req->dbo->m_hStmt );
    uv_mutex_unlock(&Database::g_odbcMutex);
  }
  
  SQLRETURN ret = SQLTables( 
    prep_req->dbo->m_hStmt, 
    (SQLCHAR *) prep_req->catalog,   SQL_NTS, 
    (SQLCHAR *) prep_req->schema,   SQL_NTS, 
    (SQLCHAR *) prep_req->table,   SQL_NTS, 
    (SQLCHAR *) prep_req->type,   SQL_NTS
  );
  
  // this will be checked later in UV_AfterQuery
  prep_req->result = ret; 
}

Handle<Value> Database::Tables(const Arguments& args) {
  HandleScope scope;

  REQ_STR_OR_NULL_ARG(0, catalog);
  REQ_STR_OR_NULL_ARG(1, schema);
  REQ_STR_OR_NULL_ARG(2, table);
  REQ_STR_OR_NULL_ARG(3, type);
  Local<Function> cb = Local<Function>::Cast(args[4]);

  Database* dbo = ObjectWrap::Unwrap<Database>(args.This());
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  query_request* prep_req = (query_request *) calloc(1, sizeof(query_request));
  
  if (!prep_req) {
    V8::LowMemoryNotification();
    return ThrowException(Exception::Error(String::New("Could not allocate enough memory")));
  }

  prep_req->sql = NULL;
  prep_req->catalog = NULL;
  prep_req->schema = NULL;
  prep_req->table = NULL;
  prep_req->type = NULL;
  prep_req->column = NULL;
  prep_req->cb = Persistent<Function>::New(cb);

  if (!String::New(*catalog)->Equals(String::New("null"))) {
    prep_req->catalog = (char *) malloc(catalog.length() +1);
    strcpy(prep_req->catalog, *catalog);
  }
  
  if (!String::New(*schema)->Equals(String::New("null"))) {
    prep_req->schema = (char *) malloc(schema.length() +1);
    strcpy(prep_req->schema, *schema);
  }
  
  if (!String::New(*table)->Equals(String::New("null"))) {
    prep_req->table = (char *) malloc(table.length() +1);
    strcpy(prep_req->table, *table);
  }
  
  if (!String::New(*type)->Equals(String::New("null"))) {
    prep_req->type = (char *) malloc(type.length() +1);
    strcpy(prep_req->type, *type);
  }
  
  prep_req->dbo = dbo;
  work_req->data = prep_req;
  
  uv_queue_work(uv_default_loop(), work_req, UV_Tables, (uv_after_work_cb)UV_AfterQuery);

  dbo->Ref();

  return scope.Close(Undefined());
}

void Database::UV_Columns(uv_work_t* req) {
  query_request* prep_req = (query_request *)(req->data);
  
  if(prep_req->dbo->m_hStmt)
  {
    SQLFreeHandle( SQL_HANDLE_STMT, prep_req->dbo->m_hStmt );
    SQLAllocStmt(prep_req->dbo->m_hDBC,&prep_req->dbo->m_hStmt );
  }
  
  SQLRETURN ret = SQLColumns( 
    prep_req->dbo->m_hStmt, 
    (SQLCHAR *) prep_req->catalog,   SQL_NTS, 
    (SQLCHAR *) prep_req->schema,   SQL_NTS, 
    (SQLCHAR *) prep_req->table,   SQL_NTS, 
    (SQLCHAR *) prep_req->column,   SQL_NTS
  );
  
  // this will be checked later in UV_AfterQuery
  prep_req->result = ret;
}

Handle<Value> Database::Columns(const Arguments& args) {
  HandleScope scope;

  REQ_STR_OR_NULL_ARG(0, catalog);
  REQ_STR_OR_NULL_ARG(1, schema);
  REQ_STR_OR_NULL_ARG(2, table);
  REQ_STR_OR_NULL_ARG(3, column);
  Local<Function> cb = Local<Function>::Cast(args[4]);
  
  Database* dbo = ObjectWrap::Unwrap<Database>(args.This());
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  query_request* prep_req = (query_request *) calloc(1, sizeof(query_request));
  
  if (!prep_req) {
    V8::LowMemoryNotification();
    return ThrowException(Exception::Error(String::New("Could not allocate enough memory")));
  }

  prep_req->sql = NULL;
  prep_req->catalog = NULL;
  prep_req->schema = NULL;
  prep_req->table = NULL;
  prep_req->type = NULL;
  prep_req->column = NULL;
  prep_req->cb = Persistent<Function>::New(cb);

  if (!String::New(*catalog)->Equals(String::New("null"))) {
    prep_req->catalog = (char *) malloc(catalog.length() +1);
    strcpy(prep_req->catalog, *catalog);
  }
  
  if (!String::New(*schema)->Equals(String::New("null"))) {
    prep_req->schema = (char *) malloc(schema.length() +1);
    strcpy(prep_req->schema, *schema);
  }
  
  if (!String::New(*table)->Equals(String::New("null"))) {
    prep_req->table = (char *) malloc(table.length() +1);
    strcpy(prep_req->table, *table);
  }
  
  if (!String::New(*column)->Equals(String::New("null"))) {
    prep_req->column = (char *) malloc(column.length() +1);
    strcpy(prep_req->column, *column);
  }
  
  prep_req->dbo = dbo;
  work_req->data = prep_req;
  
  uv_queue_work(uv_default_loop(), work_req, UV_Columns, (uv_after_work_cb) UV_AfterQuery);
  
  dbo->Ref();

  return scope.Close(Undefined());
}

Persistent<FunctionTemplate> Database::constructor_template;

extern "C" void init (v8::Handle<Object> target) {
  Database::Init(target);
}

NODE_MODULE(odbc_bindings, init)