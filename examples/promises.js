const { Database, open } = require('../');

async function main() {
    const db = await open('DRIVER={HFSQL};SERVER=DESKTOP-3EM4HH9;UID=Admin;PWD=;DATABASE=KeziaII');

    const res = await db.query('SELECT 1 + 1 as res');

    console.debug(res);
}

main();
