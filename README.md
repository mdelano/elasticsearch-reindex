# elasticsearch-reindex
Script for migrating indexes in ElasticSearch

#### Install
```bash
$ git clone git@github.com:mdelano/elasticsearch-reindex.git
$ npm install -g
$ es-reindex -h


  Usage: es-reindex [options]

  Options:

    -h, --help                     output usage information
    -V, --version                  output the version number
    -s, --source-host <s>          The protocol(optional), host, and port or the source elasticsearch cluster. Required.
    --source-host-verion <s>       The version of the source elasticsearch cluster. Defaults to 2.3.
    -d, --destination-host <s>     The protocol(optional), host, and port or the destination elasticsearch cluster. Required.
    --destination-host-verion <s>  The version of the destination elasticsearch cluster. Defaults to 2.3.
    -i, --index <s>                The name of the index. Required.
    -t, --type <s>                 The name of the type in the index. Optional.
    -b, --batch-size <n>           The amount of documents to process at once. Defaults to 1000.
    -o, --skip <n>                 The number of documents to skip. Defaults to 0.
    -T, --scroll-timeout <s>       How long to keep the scroll open. Defaults to 30s.
    -q, --query <s>                A custom query. Defaults to '{match_all: {}}'.
    -S, --sort <s>                 A custom sort. Defaults to '["_doc"]'.
    -l, --log-level <s>            error, warn, info, verbose, debug, silly 
```
