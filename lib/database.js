const { ODBC } = require('./bindings');

class Database {
    constructor(options) {
        this.odbc = new ODBC();
        this.connected = false;

        // TODO Handle fetch Mode
        this.fetchMode = options.fetchMode;
        this.connectTimeout = options.connectTimeout;
        this.loginTimeout = options.loginTimeout;
    }

    async open(connectionString) {
        if (typeof connectionString === 'object') {
            connectionString = Object
                .keys(connectionString)
                .reduce((acc, key) => `${acc}${key}=${connectionString[key]};`);
        }

        this.co = await this.odbc.createConnection();

        if (this.connectTimeout || this.connectTimeout === 0) this.co.connectTimeout = this.connectTimeout;
        if (this.loginTimeout || this.loginTimeout === 0) this.co.loginTimeout = this.loginTimeout;

        const res = await this.co.open(connectionString);

        this.connected = true;

        return res;
    }

    async close() {
        if (!this.co) return;

        try {
            await this.co.close();
        } catch (e) {
        }
        this.connected = false;
        delete this.co;
    }

    async query(sql, params) {
        this.assertConnection();

        if (params) {

        } else {

        }
        return this.co.query(sql, params);
    }

    async beginTransaction() {
        return this.co.beginTransaction();
    }

    async endTransaction(rollback = false) {
        return this.co.endTransaction(rollback);
    }

    async commitTransaction() {
        return this.endTransaction(false);
    }

    async rollbackTransaction() {
        return this.endTransaction(true);
    }

    async columns(catalog, schema, table, column) {
        const res = await this.co.columns(catalog, schema, table, column);
        return res.fetchAll();
    }

    async tables(catalog, schema, table, type) {
        const res = await this.co.tables(catalog, schema, table, type);
        return res.fetchAll();
    }

    async prepare(sql) {
        const stmt = await this.co.createStatement();
        return stmt.prepare(sql);
    }

    assertConnection() {
        if (this.connected) return;
        throw new Error('You must call Database#open first');
    }
}

module.exports = Database;
