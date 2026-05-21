# Etcd Manifest Metadata Provider

This provider stores remote Lucene file manifest records in etcd. It is intentionally scoped to remote-search metadata:

- committed file identity
- remote object key
- metadata epoch
- upload status: `DIRTY`, `UPLOADING`, `CLEAN`, `PINNED`
- commit snapshot generations
- PIT snapshot pins

Index settings, mappings, lifecycle policies, and shard routing live in cluster state, not in this provider.
