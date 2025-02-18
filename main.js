import { sqlite3Worker1Promiser } from '@sqlite.org/sqlite-wasm';
import mkKnex from 'knex';
import van from 'vanjs-core';

const {label, input, div, span, a, button, select, option, table, tbody, thead, tr, td, th, h3, img} = van.tags;
const knex = mkKnex({client: 'sqlite3'});

const databasePath = new URL('cfr-db.sqlite', import.meta.url);
const pdfSvgPath = new URL('pdf.svg', import.meta.url);

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

    const agencyFilter = van.state('');
    const titleFilter = van.state('');
    const partFilter = van.state('');
    const sectionFilter = van.state('');

    function filterObj() {
	return {
	    agency: agencyFilter.val,
	    title: titleFilter.val,
	    part: partFilter.val,
	    section: sectionFilter.val,
	};
    }

    function applyFilterObj(filterObj) {
	agencyFilter.val = filterObj.agency;
	titleFilter.val = filterObj.title;
	partFilter.val = filterObj.part;
	sectionFilter.val = filterObj.section;
    }

    let granularity = van.state("agency");
    let sortKey = van.state("num_cases");
    let limit = van.state(100);
    let pdfsLimit = van.state(20);

    let queryResults = van.state([]);
    let pdfs = van.state([]);

    van.derive(() => {
	const queryText = theQuery(filterObj(), granularity.val, sortKey.val, limit.val);
	console.log(queryText);
	ez_query(queryText).then(res => queryResults.val = res);
    });

    van.derive(() => {
	ez_query(pdfsQuery(filterObj(), pdfsLimit.val))
	    .then(res => pdfs.val = res);
    });

    const allAgencies = await ez_query(agencyListQuery());

    function selectorsDisplay() {

	function btn(stateVar, value, text) {
	    return button({class: () => (stateVar.val == value ? 'active ' : '') + 'btn', onclick: () => stateVar.val = value}, text);
	}

	function clearFilter() {
	    agencyFilter.val = titleFilter.val = partFilter.val = sectionFilter.val = '';
	}

	function clearAgencyFilter() {
	    agencyFilter.val = '';
	}

	function clearSectionFilter() {
	    titleFilter.val = partFilter.val = sectionFilter.val = '';
	}

	function sectionFilterInput(name, stateVar, disableUnlessSet = []) {
	    return input({placeholder: name,
			  size: 2,
			  value: stateVar,
			  disabled: () => !disableUnlessSet.every(v => v.val != ''),
			  oninput: ev => {stateVar.val = ev.target.value; clearAgencyFilter();}});
	}

	return div(
	    div({class: 'form-group'},
		label({class: 'form-label text-bold'}, "Filter: "),
		sectionFilterInput('title', titleFilter),
		" CFR § ",
		sectionFilterInput('part', partFilter, [titleFilter]),
		".",
		sectionFilterInput('sec', sectionFilter, [titleFilter, partFilter]),
		" OR Agency: ",
		select({id: 'agency-select', onchange: ev => {agencyFilter.val = ev.target.value; clearSectionFilter();}},
		    option({value: '', selected: () => !agencyFilter.val}, "(no filter)"),
		    allAgencies.map(agency => option({value: agency.agency, selected: () => agencyFilter.val == agency.agency}, agency.agency)),
		),
		div(a({onclick: clearFilter, href: 'javascript:void();'}, "Clear All Filters")),
	       ),
	    div({class: 'form-group'},
		label({class: 'form-label text-bold'}, "Search Granularity:"),
		div({class: 'btn-group'},
		    btn(granularity, 'title', "Title"),
		    btn(granularity, 'part', "Part"),
		    btn(granularity, 'section', "Section"),
		    btn(granularity, 'agency', "Agency"),
	       ),
	       ),
	    div({class: 'form-group'},
		label({class: 'form-label text-bold'}, "Sort key:"),
		div({class: 'btn-group'},
		    btn(sortKey, 'num_words', "Number of words"),
		    btn(sortKey, 'num_cases', "Number of court cases"),
		    btn(sortKey, 'case_word_ratio', "Court cases per word"),
		    
		   )
	       ),
	);
    }

    function queryDisplay() {
	return () => {
	    const maxValue = Math.max.apply(Math, queryResults.val.map(row => machineReadableValue(row, sortKey.val)));
	    return table({id: 'data-table', cellspacing: 0},
			 thead(
			     tr(th("Location"), th(sortKeyDescription(sortKey.val)))
			 ),
			 tbody(queryResults.val.map(result => {
			     const fraction = machineReadableValue(result, sortKey.val) / maxValue;

			     function onclick() {
				 applyFilterObj(newFilterObject(result, granularity.val));
				 granularity.val = nextGranularity(granularity.val);
			     }

			     return tr({style: `background: linear-gradient(to right, rgba(0,0,0,.15) ${fraction * 100}%, transparent ${fraction * 100}%)`},
				       td(a({href: 'javascript:void();', onclick}, humanReadableKey(result, granularity.val))),
				       td({class: 'number-column'}, humanReadableValue(result, sortKey.val)));
			 })));
	};
    }

    function pdfLinksDisplay() {
	return () =>
	div(h3("Matching Cases"),
	    pdfs.val.map(pdf =>
		div(a({href: `https://www.govinfo.gov/content/pkg/${pdf.package_id}/pdf/${pdf.granule_id}.pdf`,
		       target: "_blank",
		       class: 'pdf-link'},
		      img({class: 'pdf-img', src: pdfSvgPath}),
		      pdf.case_title),
		    ' ',
		    span({class: 'text-gray'}, pdf.date_opinion_issued))));
    }

    van.add(document.getElementById('selectors'), selectorsDisplay());
    van.add(document.getElementById('results'), queryDisplay());
    van.add(document.getElementById('pdf-links'), pdfLinksDisplay());
}

go();

function theQuery(filter, granularity, sortKey, limit) {
    let sectionAndAgencyQuery = knex('cfr_section');

    if (filter.agency || granularity === "agency") {
	// only apply if we need it, otherwise there are a few chapters that belong to multiple
	// agencies, and the join would cause those rows to be duplicated and thus double counted
	// when counting num_words. This is quite a hack and i am ashamed of it
	sectionAndAgencyQuery.leftJoin('cfr_agency', function() {
	    this.on('cfr_section.title', '=', 'cfr_agency.title')
	        .on('cfr_section.chapter', '=', 'cfr_agency.chapter');
	});
    }

    let columns;
    switch(granularity) {
    case "title": columns = ['cfr_section.title']; break;
    case "part": columns = ['cfr_section.title', 'cfr_section.part']; break;
    case "section": columns = ['cfr_section.title', 'cfr_section.part', 'cfr_section.section', 'cfr_section.description']; break;
    case "agency": columns = ['cfr_agency.agency']; break;
    default: throw new Error(`Unknown granularity ${granularity}`);
    }

    // apply filter
    const wordsQuery = queryApplyFilter(sectionAndAgencyQuery.clone(), filter)
	  .select(columns)
	  .groupBy(columns)
	  .sum('cfr_section.num_words as num_words');

    const allTablesQuery = sectionAndAgencyQuery.clone()
	  .leftJoin('cfr_pdf', function() {
	      this.on('cfr_section.title', '=', 'cfr_pdf.title')
		  .on('cfr_section.part', '=', 'cfr_pdf.part')
		  .on('cfr_section.section', '=', 'cfr_pdf.section');
	  })
	  .leftJoin('court_opinion_pdf', 'cfr_pdf.granule_id', 'court_opinion_pdf.granule_id');

    const numCasesQuery = queryApplyFilter(allTablesQuery.clone(), filter)
	  .select(columns)
	  .groupBy(columns)
	  .countDistinct('court_opinion_pdf.package_id as num_cases');


    // apply sort key (which is not just for sorting; also for which metric to compute)
    let sortedQuery;
    switch(sortKey) {
    case "num_cases":
	sortedQuery = numCasesQuery.orderBy('num_cases', 'desc');
	break;
    case "num_words":
	sortedQuery = wordsQuery.orderBy('num_words', 'desc');
	break;
    case "case_word_ratio":
	sortedQuery = knex.select(columns.map(col => 'wq.' + col.split('.')[1]))
	    .from(wordsQuery.clone().as('wq'))
	    .join(numCasesQuery.clone().as('ncq'), function() {
		columns.forEach(col => this.on('wq.' + col.split('.')[1], '=', 'ncq.' + col.split('.')[1]));
	    })
	    .select(knex.raw('CAST(num_cases as REAL) / CAST(num_words as REAL) as case_word_ratio'))
	    .orderBy('case_word_ratio', 'desc');
	break;
    default:
	throw new Error(`Unknown sort key ${sortKey}`);
    }

    // join it with appropriate description table
    let describedQuery;
    switch(granularity) {
    case "title":
	describedQuery = sortedQuery.joinRaw('JOIN cfr_title USING (title)')
	    .select('cfr_title.description as title_description');
	break;
    case "part":
	describedQuery = sortedQuery.joinRaw('JOIN cfr_part USING (title, part)')
	    .select('cfr_part.description as part_description');
	break;
    case "section":
	// awful awful last-minute hack here, i'm so sorry
	if (sortKey == "case_word_ratio") {
	    describedQuery = sortedQuery.select("wq.description as section_description");
	} else {
	    describedQuery = sortedQuery.select("cfr_section.description as section_description");
	}
	break;
    case "agency":
	describedQuery = sortedQuery;
	break;
    default:
	throw new Error(`Unknown granularity ${granularity}`);
    }

    return describedQuery.limit(limit).toString();
}

function queryApplyFilter(query, filter) {
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
    return query;
}

function ellipsize(str, len = 40) {
	if (str.length <= len) {
	    return str;
	} else {
	    return str.slice(0, len - 3) + '...';
	}
}

function humanReadableKey(queryRes, granularity) {
    switch (granularity) {
    case "title": return `${queryRes.title} CFR: ${ellipsize(queryRes.title_description)}`;
    case "part": return `${queryRes.title} CFR ${queryRes.part}: ${ellipsize(queryRes.part_description)}`;
    case "section": return `${queryRes.title} CFR § ${queryRes.part}.${queryRes.section}: ${ellipsize(queryRes.section_description)}`;
    case "agency": return queryRes.agency;
    default:
	throw new Error(`Unrecognized granularity: ${granularity}`);
    }
}

function newFilterObject(queryRes, granularity) {
    const result = {agency: '', title: '', part: '', section: ''};

    switch(granularity) {
    case "title": result.title = queryRes.title; break;
    case "part": result.title = queryRes.title; result.part = queryRes.part; break;
    case "section": result.title = queryRes.title; result.part = queryRes.part; result.section = queryRes.section; break;
    case "agency": result.agency = queryRes.agency; break;
    default:
	throw new Error(`Unrecognized granularity: ${granularity}`);
    }

    return result;
}

function nextGranularity(last_granularity) {
    switch(last_granularity) {
    case "title": return "part";
    case "part": return "section";
    case "agency": return "part";
    default:
	throw new Error(`No next granularity for ${last_granularity}`);
    }

    throw new Error(`Unknown granularity ${last_granularity}`);
}

function sortKeyDescription(sortKey) {
    switch(sortKey) {
    case "num_cases": return "# Court Cases";
    case "num_words": return "# Words";
    case "case_word_ratio": return "Cases per 1000 Words";
    default: throw new Error(`Unknown sort key ${sortKey}`);
    }
}

function machineReadableValue(queryRes, sortKey) {
    switch (sortKey) {
    case "num_words": return queryRes.num_words;
    case "num_cases": return queryRes.num_cases;
    case "case_word_ratio": return queryRes.case_word_ratio;
    default:
	throw new Error(`Unrecognized sort key: ${sortKey}`);
    }
}

function humanReadableValue(queryRes, sortKey) {
    switch (sortKey) {
    case "num_words": return `${queryRes.num_words} Words`;
    case "num_cases": return `${queryRes.num_cases} Cases`;
    case "case_word_ratio": return `${((queryRes.case_word_ratio || 0)*1000).toFixed(2)}`;
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
	.distinct() // will handle the occasional case of duplicated cfr_agency
	.orderBy('court_opinion_pdf.date_opinion_issued', 'desc')
	.limit(limit);

    query = queryApplyFilter(query, filter);

    return query.toString();
}

function agencyListQuery() {
    return knex('cfr_agency').select('agency').distinct().toString();
}
