# Metadata

This package stores remote Lucene file manifest metadata.

Index settings, mappings, lifecycle policy, node membership, and shard routing are stored in cluster state. The metadata
provider layer tracks:

- committed Lucene file records and upload status
- complete commit snapshot generations for searchable snapshot reads
- short-lived snapshot pins used by PIT

Live file records may move through `DIRTY` and `UPLOADING`. A commit snapshot is published only after all files visible
to a committed `segments_N` are remote-readable.
