# Metadata

This package stores remote Lucene file manifest metadata.

Index settings, mappings, lifecycle policy, node membership, and shard routing are stored in cluster state. The metadata
provider layer only tracks committed Lucene file records and their upload status for searchable snapshot reads.
