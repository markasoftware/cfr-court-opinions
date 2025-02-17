import { sqlite3Worker1Promiser } from '@sqlite.org/sqlite-wasm';
import mkKnex from 'knex';
import van from 'vanjs-core';

const {label, input, div, span, a} = van.tags;
const knex = mkKnex({client: 'sqlite3'});

const databasePath = new URL('cfr-db.sqlite', import.meta.url);

const granularities = ["title", "chapter", "part", "section", "agency"];

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

    const filter = van.state({
	agency: null,
	title: null,
	part: null,
	section: null,
    });
    let granularity = van.state("title");
    let sortKey = van.state("case_word_ratio");
    let limit = van.state(100);
    let pdfsLimit = van.state(100);

    let queryResults = van.state([]);
    let pdfs = van.state([]);

    van.derive(() => {
	ez_query(theQuery(filter.val, granularity.val, sortKey.val, limit.val))
	    .then(res => queryResults.val = res);
    });

    van.derive(() => {
	ez_query(pdfsQuery(filter.val, pdfsLimit.val))
	    .then(res => pdfs.val = res);
    });

    function selectorsDisplay() {
	return div(
	    div("Granularity: ",
		span({onclick: () => granularity.val = 'title'}, "Title"),
		" ",
		span({onclick: () => granularity.val = 'part'}, "Part"),
		" ",
		span({onclick: () => granularity.val = 'section'}, "Section"),
	       )
	);
    }

    function queryDisplay() {
	return () =>
	div(queryResults.val.map(result =>
	    div(humanReadableKey(result, granularity.val), " ", humanReadableValue(result, sortKey.val))));
    }

    function pdfLinksDisplay() {
	return () =>
	div(pdfs.val.map(pdf =>
	    div(a({href: `https://www.govinfo.gov/content/pkg/${pdf.package_id}/pdf/${pdf.granule_id}.pdf`},
		  pdf.case_title))));
    }

    van.add(document.getElementById('selectors'), selectorsDisplay());
    van.add(document.getElementById('results'), queryDisplay());
    van.add(document.getElementById('pdf-links'), pdfLinksDisplay());
}

go();

function nextGranularity(last_granularity) {
    switch(last_granularity) {
    case "title": return "chapter";
    case "chapter": return "part";
    case "part": return "section";
    case "agency": return "part";
    default: return null;
    }

    throw new Error(`Unknown granularity ${last_granularity}`);
}

function theQuery(filter, granularity, sortKey, limit) {
    let query = knex('cfr_section')
	.leftJoin('cfr_pdf', function() {
	    this.on('cfr_section.title', '=', 'cfr_pdf.title')
		.on('cfr_section.part', '=', 'cfr_pdf.part')
		.on('cfr_section.section', '=', 'cfr_pdf.section');
	})
	.leftJoin('court_opinion_pdf', 'cfr_pdf.granule_id', 'court_opinion_pdf.granule_id')
	.leftJoin('cfr_agency', function() {
	    this.on('cfr_section.title', '=', 'cfr_agency.title')
	        .on('cfr_section.chapter', '=', 'cfr_agency.chapter');
	})
	.limit(limit);

    // apply filter
    if (filter.agency) {
	query = query.where('cfr_agency.agency', filter.agency);
    } else {
	if (filter.title) {
	    query = query.where('cfr_section.title', filter.title);
	    if (filter.part) {
		query = query.where('cfr_section.part', filter.part);
		if (filter.section) {
		    query = query.where('cfr_section.section', filter.section);
		}
	    }
	}
    }

    // apply granularity
    let columns;
    switch(granularity) {
    case "title": columns = ['cfr_section.title']; break;
    case "chapter": columns = ['cfr_section.title', 'cfr_section.chapter']; break;
    case "part": columns = ['cfr_section.title', 'cfr_section.chapter', 'cfr_section.part']; break;
    case "section": columns = ['cfr_section.title', 'cfr_section.chapter', 'cfr_section.part', 'cfr_section.section']; break;
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
	query = query.sum('cfr_section.num_words as num_words').orderBy('num_words', 'desc');
	break;
    case "case_word_ratio":
	query = query
	    .select(knex.raw('COUNT(DISTINCT court_opinion_pdf.package_id) / CAST(num_words as REAL) as case_word_ratio'))
	    .orderBy('case_word_ratio', 'desc');
	break;
    default:
	throw new Error(`Unknown sort key ${sortKey}`);
    }

    return query.toString();
}

function humanReadableKey(queryRes, granularity) {
    switch (granularity) {
    case "title": return `CFR ${queryRes.title}`;
    case "chapter": return `CFR ${queryRes.title} Ch. ${queryRes.chapter}`;
    case "part": return `${queryRes.title} CFR ${queryRes.part}`;
    case "section": return `${queryRes.title} CFR § ${queryRes.part}.${queryRes.section}`;
    case "agency": return queryRes.agency;
    default:
	throw new Error(`Unrecognized granularity: ${granularity}`);
    }
}

function humanReadableValue(queryRes, sortKey) {
    switch (sortKey) {
    case "num_words": return `${queryRes.num_words} Words`;
    case "num_cases": return `${queryRes.num_cases} Cases`;
    case "case_word_ratio": return `${((queryRes.case_word_ratio || 0)*1000).toFixed(2)} Cases/Thousand Words`;
    default:
	throw new Error(`Unrecognized sort key: ${sortKey}`);
    }
}

function pdfsQuery(filter, limit) {
    let query = knex('cfr_section')
	.join('cfr_pdf', function() {
	    this.on('cfr_section.title', '=', 'cfr_pdf.title')
		.on('cfr_section.part', '=', 'cfr_pdf.part')
		.on('cfr_section.section', '=', 'cfr_pdf.section');
	})
	.join('court_opinion_pdf', 'cfr_pdf.granule_id', 'court_opinion_pdf.granule_id')
	.leftJoin('cfr_agency', function() {
	    this.on('cfr_section.title', '=', 'cfr_agency.title')
	        .on('cfr_section.chapter', '=', 'cfr_agency.chapter');
	})
	.select('court_opinion_pdf.*')
	.distinct()
	.orderBy('court_opinion_pdf.date_opinion_issued')  // just for stability
	.limit(limit);

    if (filter.agency) {
	query = query.where('cfr_agency.agency', filter.agency);
    } else {
	if (filter.title) {
	    query = query.where('cfr_section.title', filter.title);
	    if (filter.part) {
		query = query.where('cfr_section.part', filter.part);
		if (filter.section) {
		    query = query.where('cfr_section.section', filter.section);
		}
	    }
	}
    }

    return query.toString();
}
