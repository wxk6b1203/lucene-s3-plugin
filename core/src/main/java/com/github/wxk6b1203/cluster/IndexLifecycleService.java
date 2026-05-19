package com.github.wxk6b1203.cluster;

import java.io.IOException;

public interface IndexLifecycleService {
    ClusterState putPolicy(IndexLifecyclePolicy policy) throws IOException;

    ClusterState attachPolicy(String indexName, String policyName) throws IOException;
}
