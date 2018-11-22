const bindings = require('bindings')('odbc_bindings');

module.exports = {
    ODBC: bindings.ODBC,
    ODBCConnection: bindings.ODBC,
    ODBCStatement: bindings.ODBCStatement,
    ODBCResult: bindings.ODBCResult,

    // Constants
    FETCH_ARRAY: bindings.FETCH_ARRAY,
    SQL_USER_NAME: bindings.SQL_USER_NAME,

    // dynodbc
    // loadODBCLibrary: bindings.loadODBCLibrary,
};
