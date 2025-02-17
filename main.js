import { sqlite3Worker1Promiser } from '@sqlite.org/sqlite-wasm';

const databasePath = new URL('cfr-db.sqlite', import.meta.url);

async function go() {
    const promiser = await new Promise((resolve) => {
	const _promiser = sqlite3Worker1Promiser({
	    onready: () => resolve(_promiser),
	});
    });

    async function ez_query(sql, bind = []) {
	return (await promiser('exec', {sql, bind, rowMode: 'object', returnValue: 'resultRows'})).result.resultRows;
    }

    // download the sqlite database itself
    const response = await fetch(databasePath);
    const arrayBuffer = await response.arrayBuffer();

    // write it to opfs
    const opfsRoot = await navigator.storage.getDirectory();
    const fh = await opfsRoot.getFileHandle('cfr-db.sqlite', { create: true });
    const writable = await fh.createWritable();
    writable.write(arrayBuffer);
    writable.close();

    await promiser('open', {filename: 'cfr-db.sqlite', vfs: 'opfs'});
    console.log(await ez_query('SELECT * FROM cfr_pdf LIMIT 10'));
}

go();
