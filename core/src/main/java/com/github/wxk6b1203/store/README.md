# Store

This package contains the Lucene `Directory` implementation and object-store adapter layer.

Local files are organized by physical shard name:

```text
<data-path>/
  _wal/<index>__shard_<n>/_data/       # current owner local Lucene writer files
  _shared/<index>__shard_<n>/_data/    # local cache for remote clean/pinned files
  _shared/<index>__shard_<n>/_temp/    # temporary remote download files
```

`S3CachingDirectory` writes to `_wal` and reads from `_wal`, then `_shared`, then the configured
`RemoteObjectStore`. `ManifestManager` publishes committed Lucene files to the remote store and
tracks file status in `ManifestMetadataManager`.
