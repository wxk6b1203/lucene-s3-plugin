package com.github.wxk6b1203.config;

public interface S3Configs {
    String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    String S3_BUCKET_NAME_CONFIG_DESCRIPTION = "The name of the S3 bucket to store files.";

    String S3_REGION_CONFIG = "s3.region";
    String S3_REGION_CONFIG_DESCRIPTION = "The AWS region where the S3 bucket is located.";

    String S3_ACCESS_KEY_CONFIG = "s3.access.key";
    String S3_ACCESS_KEY_CONFIG_DESCRIPTION = "The AWS access key for S3 authentication.";

    String S3_SECRET_KEY_CONFIG = "s3.secret.key";
    String S3_SECRET_KEY_CONFIG_DESCRIPTION = "The AWS secret key for S3 authentication.";

    String S3_ENDPOINT_CONFIG = "s3.endpoint";
    String S3_ENDPOINT_CONFIG_DESCRIPTION = "The custom endpoint for S3-compatible storage services.";
}
