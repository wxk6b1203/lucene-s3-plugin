package com.github.wxk6b1203.store.common;

import com.github.wxk6b1203.common.Common;
import com.github.wxk6b1203.store.directory.Hierarchy;

import java.nio.file.Path;

public class PathUtil {
    public static Path walDataPath(Path basePath, String indexName) {
        return basePath.resolve(Hierarchy.WAL.path)
                .resolve(indexName)
                .resolve(Hierarchy.DATA.path)
                ;
    }

    public static Path walStatePath(Path basePath, String indexName) {
        return basePath.resolve(Hierarchy.WAL.path)
                .resolve(indexName)
                .resolve(Hierarchy.STATE.path)
                ;
    }

    public static Path sharedDataPath(Path basePath, String indexName) {
        return basePath.resolve(Hierarchy.SHARED.path)
                .resolve(indexName)
                .resolve(Hierarchy.DATA.path)
                ;
    }

    public static Path sharedStatePath(Path basePath, String indexName) {
        return basePath.resolve(Hierarchy.SHARED.path)
                .resolve(indexName)
                .resolve(Hierarchy.STATE.path)
                ;
    }

    public static String s3ObjectKey(String indexName, String name) {
        return indexName + Common.SLASH + Hierarchy.DATA.path + Common.SLASH + name;
    }
}
