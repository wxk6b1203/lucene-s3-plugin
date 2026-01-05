package com.github.wxk6b1203.store.config;

public interface StoreConfigKeys {
    String BUCKET_CONFIG = "s3.bucket.name";
    String BUCKET_CONFIG_DESCRIPTION = "The name of the S3 bucket to store files.";

    String CACHE_PATH_CONFIG = "local.cache.path";
    String CACHE_PATH_CONFIG_DESCRIPTION = "Local cache storage path";

    String CACHE_MAX_SIZE_CONFIG = "local.cache.max.size.mb";
    String CACHE_MAX_SIZE_CONFIG_DESCRIPTION = "Local cache maximum size in MB";

    String CACHE_MAX_DISK_USAGE_PERCENTAGE_CONFIG = "local.cache.max.disk.usage.percentage";
    String CACHE_MAX_DISK_USAGE_PERCENTAGE_CONFIG_DESCRIPTION = "Local cache maximum disk usage percentage(0-100)";


}
