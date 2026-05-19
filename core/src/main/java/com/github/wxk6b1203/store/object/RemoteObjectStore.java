package com.github.wxk6b1203.store.object;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

public interface RemoteObjectStore {
    void put(String key, Path source) throws IOException;

    void get(String key, Path target) throws IOException;

    void delete(Collection<String> keys) throws IOException;
}
