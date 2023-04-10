# bleveparquet - a readonly parquet upsidedown store for bleve

This is an experimental upsidedown storage engine for bleve that
uses parquet for the index format.

This store does not support online updates, instead an existing
bleve index can be exported to parquet using the `dump-to-parquet` tool.

The point of this was to make a format that could be stored in an object
store like s3 and streamed on demand to a lambda function using
[httpreadat](https://github.com/psanford/httpreadat). This works quite well
if you are willing to sacrifice some query latency. Actually hooking it up
to bleve is a little clunky because the bleve api expects to find files on disk
(mainly the `index_mata.json` file).

Performance was roughly similar to using an sqlite db stored in s3. A good
caching strategy helps quite a bit in making either of these implementations
reasonably usable.
