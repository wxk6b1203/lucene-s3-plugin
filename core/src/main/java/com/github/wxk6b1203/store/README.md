# Store

The index WAL and persistent storage implementation.

## Architecture

The file storage structure is organized as follows:
```text
/{root}/
    |-- wal/{indexName}/ # Write-Ahead Log files for each index
                |-- _data/ # Persistent storage files for each index
                |-- _meta/ # Metadata files for each index
                |-- _temp/ # Temporary files
    |-- shared/ # Shared resources across indexes
          |-- cache/ # Cache files
          |-- config/ # Configuration files
```

The metadata structure is organized as follows:
```text
/{root}/{node_id}/
