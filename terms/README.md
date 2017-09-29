# Helpful hints

## Viewing *.jsonl.gz files

The easiest to use command to view the [JSONLines](http://jsonlines.org/) files
is:

    gunzip -c <REPLACEME>.jsonl.gz | jq . | more

This basically gunzips the file pipes it to STDOUT.  [jq](https://stedolan.github.io/jq/)
is a command line JSON manipulator.  It takes STDIN from the pipe and provides
default formatting processing to it (pretty-prints).
