import { sqlite3Worker1Promiser } from '@sqlite.org/sqlite-wasm';
import mkKnex from 'knex';
import van from 'van';

const {label, input, div} = van.tags;
const knex = mkKnex({client: 'sqlite3'});

const databasePath = new URL('cfr-db.sqlite', import.meta.url);

const granularities = ["title", "chapter", "part", "subpart", "agency"];

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
    // TODO only download if not already in opfs
    const response = await fetch(databasePath);
    const arrayBuffer = await response.arrayBuffer();

    // write it to opfs
    const opfsRoot = await navigator.storage.getDirectory();
    const fh = await opfsRoot.getFileHandle('cfr-db.sqlite', { create: true });
    const writable = await fh.createWritable();
    writable.write(arrayBuffer);
    writable.close();

    await promiser('open', {filename: 'cfr-db.sqlite', vfs: 'opfs'});
    console.log('SQLite database open.');

    const queryDisplay = () => div(theQuery({agency: "FAA"}, "chapter", "num_cases"));

    van.add(document.getElementById('results'), queryDisplay);
}

go();

function nextGranularity(last_granularity) {
    switch(last_granularity) {
    case "title": return "chapter";
    case "chapter": return "part";
    case "part": return "subpart";
    case "agency": return "part";
    default: return null;
    }

    throw new Error(`Unknown granularity ${last_granularity}`);
}

// the query must involve the CfrSubpart table
function theQuery(filter, granularity, sortKey) {
    let query = knex('cfr_subpart')
	.join('cfr_pdf', function() {
	    this.on('cfr_subpart.title', '=', 'cfr_pdf.title')
		.on('cfr_subpart.part', '=', 'cfr_pdf.part')
		.on('cfr_subpart.subpart', '=', 'cfr_pdf.subpart');
	})
	.join('court_opinion_pdf', 'cfr_pdf.granule_id', 'court_opinion_pdf.granule_id')
	.join('cfr_agency', function() {
	    this.on('cfr_subpart.title', '=', 'cfr_agency.title')
	        .on('cfr_subpart.chapter', '=', 'cfr_agency.chapter');
	});

    // apply filter
    if (filter.agency) {
	query = query.where('cfr_agency.agency', filter.agency);
    } else {
	if (filter.title) {
	    query = query.where('cfr_subpart.title', filter.title);
	    if (filter.chapter) {
		query = query.where('cfr_subpart.chapter', filter.chapter);
		if (filter.part) {
		    query = query.where('cfr_subpart.part', filter.part);
		    if (filter.subpart) {
			query = query.where('cfr_subpart.subpart', filter.subpart);
		    }
		}
	    }
	}
    }

    // apply granularity
    let columns;
    switch(granularity) {
    case "title": columns = ['cfr_subpart.title']; break;
    case "chapter": columns = ['cfr_subpart.title', 'cfr_subpart.chapter']; break;
    case "part": columns = ['cfr_subpart.title', 'cfr_subpart.chapter', 'cfr_subpart.part']; break;
    case "subpart": columns = ['cfr_subpart.title', 'cfr_subpart.chapter', 'cfr_subpart.part', 'cfr_subpart.subpart']; break;
    case "agency": columns = ['cfr_agency.agency']; break;
    default: throw new Error(`Unknown granularity ${granularity}`);
    }

    query = query.select(columns).groupBy(columns);

    // apply sort key (which is not just for sorting; also for which aggregate to compute)
    switch(sortKey) {
    case "num_cases":
	query = query.countDistinct('court_opinion_pdf.package_id as num_cases').orderBy('num_cases', 'desc');
	break;
    case "num_words":
	query = query.sum('court_opinion_pdf.num_words as num_words').orderBy('num_words', 'desc');
	break;
    case "case_word_ratio":
	query = query.sum('court_opinion_pdf.num_words as num_words')
	    .countDistinct('court_opinion_pdf.package_id as num_cases')
	    .select(knex.raw('num_words / num_cases as case_word_ratio'))
	    .orderBy('case_word_ratio', 'desc');
	break;
    default:
	throw new Error(`Unknown sort key ${sortKey}`);
    }

    return query.toString();
}
