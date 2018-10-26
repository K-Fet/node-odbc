const odbc = require("../")
const cn = 'DSN=*LOCAL;UID=MARKIRISH;PWD=my1pass;CHARSET=UTF8';

console.log(odbc.SQL_CHAR);
console.log(odbc.SQL_VARCHAR);
console.log(odbc.SQL_LONGVARCHAR);

odbc.SQL_CHAR = 233;

console.log(odbc.SQL_CHAR);