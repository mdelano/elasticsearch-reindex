#!/usr/bin/env node

var DEFAULT_LOG_LEVEL = 'info';
var DEFAULT_BATCH_SIZE = 1000;
var DEFAULT_SKIP = 0;
var DEFAULT_SCROLL_TIMEOUT = '30s';
var DEFAULT_QUERY = {match_all: {}};
var DEFAULT_SORT = ["_doc"];
var DEFAULT_ELASTICSEARCH_VERSION = "2.3";





/**
 * Parse arguments
 */
var program = require('commander');

program
    .version('0.0.1')
    .option('-s, --source-host <s>', 'The protocol(optional), host, and port or the source elasticsearch cluster. Required.')
    .option('--source-host-verion <s>', 'The version of the source elasticsearch cluster. Defaults to ' + DEFAULT_ELASTICSEARCH_VERSION + '.')
    .option('-d, --destination-host <s>', 'The protocol(optional), host, and port or the destination elasticsearch cluster. Required.')
    .option('--destination-host-verion <s>', 'The version of the destination elasticsearch cluster. Defaults to ' + DEFAULT_ELASTICSEARCH_VERSION + '.')
    .option('-i, --index <s>', 'The name of the index. Required.')
    .option('-t, --type <s>', 'The name of the type in the index. Optional.')
    .option('-b, --batch-size <n>', 'The amount of documents to process at once. Defaults to ' + DEFAULT_BATCH_SIZE + '.')
    .option('-o, --skip <n>', 'The number of documents to skip. Defaults to ' + DEFAULT_SKIP + '.')
    .option('-T, --scroll-timeout <s>', 'How long to keep the scroll open. Defaults to ' + DEFAULT_SCROLL_TIMEOUT + '.')
    .option('-q, --query <s>', 'A custom query. The query must include "fields": ["_source","*"]. Defaults to ' + DEFAULT_QUERY + '.')
    .option('-S, --sort <s>', 'A custom sort. Defaults to ' + DEFAULT_SORT + '.')
    .option('-l, --log-level <s>', 'error, warn, info, verbose, debug, silly.')
    .parse(process.argv);


/**
 * Initialize logger
 */

var winston = require('winston');

var log = new (winston.Logger)({
    level: program.logLevel || DEFAULT_LOG_LEVEL,
    transports: [
        new (winston.transports.Console)({colorize: true, timestamp: true})
    ]
});





/**
 * Validate arguments and set defaults
 */
if(!program.sourceHost) {
    throw new Error("The source host is a required parameter");
}

if(!program.destinationHost) {
    throw new Error("The destination host is a required parameter");
}

if(!program.index) {
    throw new Error("The index is a required parameter");
}

var sourceHost = program.sourceHost;
var sourceHostVersion = program.sourceHostVersion || DEFAULT_ELASTICSEARCH_VERSION;
var destinationHost = program.destinationHost;
var destinationHostVersion = program.destinationHostVersion || DEFAULT_ELASTICSEARCH_VERSION;
var index = program.index;
var type = program.type || "";
var batchSize = program.batchSize || DEFAULT_BATCH_SIZE;
var skip = program.skip || DEFAULT_SKIP;
var scrollTimeout = program.scrollTimeout || DEFAULT_SCROLL_TIMEOUT;

var query;
console.log(program.query);
if(program.query) {
    query = JSON.parse(program.query)
} else {
    query = DEFAULT_QUERY;
}

var sort;

if(program.sort) {
    sort = JSON.parse(program.sort);
} else {
    sort = DEFAULT_SORT;
}

log.debug('sourceHost: ' + sourceHost);
log.debug('destinationHost: ' + destinationHost);
log.debug('index: ' + index);
log.debug('type: ' + type);
log.debug('batchSize: ' + batchSize);
log.debug('skip: ' + skip);
log.debug('scrollTimeout: ' + scrollTimeout);
log.debug('query: ' + query);
log.debug('sort: ' + sort);





/**
 * Build the ElasticSearch clients for the source and destination DB
 */
var elasticsearch = require('elasticsearch');
var client_source = new elasticsearch.Client({
    host: sourceHost,
    apiVersion: sourceHostVersion,
    log: 'warning'
});

var client_destination = new elasticsearch.Client({
    host: destinationHost,
    apiVersion: destinationHostVersion,
    log: 'warning'
});

var ttlProcessed = 0; // Keeps track of how many documents we've processed
var lastId = 0; // Keeps track of the last ID we've re-indexed

var searchOptions = {
    index: index,
    scroll: scrollTimeout,
    search_type: 'scan',
    body: {
        size: batchSize,
        from: skip,
        query: query,
        sort: sort,
        fields: ["_source","*"]
    }
};

if(type.length > 0) {
    searchOptions.type = type
}





/**
 * Begin the re-indexing
 */
log.info('Re-indexing with search ' + JSON.stringify(searchOptions));

client_source.search(searchOptions, function getMoreUntilDone(error, response) {

    log.debug('Executing search');
    if(error) {
        log.error('Failed to execute search', error);
    } else {
        log.debug('Scroll response ' + JSON.stringify(response));
        var bulk = [];

        if(response.hits.hits.length == 0) {
            client_source.scroll({
                scrollId: response._scroll_id,
                scroll: scrollTimeout
            }, getMoreUntilDone);
        } else {
            log.debug('Building bulk request from scroll response');
            for(var i = 0; i < response.hits.hits.length; i++) {
                var index = {
                    _index: response.hits.hits[i]._index,
                    _type: response.hits.hits[i]._type,
                    _id:response.hits.hits[i]._id,
                };

                // Handle child documents
                if(response.hits.hits[i].fields._parent) {
                    index.parent = response.hits.hits[i].fields._parent;
                }

                bulk.push({
                    index: index
                });

                bulk.push(response.hits.hits[i]._source);
            }

            log.debug('Indexing documents at destination via bulk API');
            client_destination.bulk({body: bulk}, function(err, result) {
                if(err) {
                    log.error('Failed to bulk load data', JSON.stringify(err));
                } else {
                    log.debug('Finished bulk request ' + JSON.stringify(result));
                    ttlProcessed+=response.hits.hits.length;
                    lastId = bulk[bulk.length - 2].index._id;
                    log.info('Reindexed ' + ttlProcessed + ' of ' + response.hits.total + ' <id:' + lastId + '>');
                }

                // If we have more documents to re-index then we'll keep going
                if (response.hits.total !== ttlProcessed) {
                    // now we can call scroll over and over
                    client_source.scroll({
                        scrollId: response._scroll_id,
                        scroll: scrollTimeout
                    }, getMoreUntilDone);
                } else {
                    log.info('Finished processing ' + ttlProcessed + ' of ' + response.hits.total + ' for search ' + JSON.stringify(searchOptions));
                    process.exit();
                }
            });
        }
    }
});

