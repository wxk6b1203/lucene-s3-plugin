package com.github.wxk6b1203.store.object;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

public class LocalFileRemoteObjectStore implements RemoteObjectStore {
    private final Path root;

    public LocalFileRemoteObjectStore(Path root) {
        this.root = root;
    }

    @Override
    public void put(String objectKey, Path source) throws IOException {
        Path target = resolve(objectKey);
        Files.createDirectories(target.getParent());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void get(String objectKey, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Files.copy(resolve(objectKey), target, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void delete(Collection<String> objectKeys) throws IOException {
        for (String objectKey : objectKeys) {
            Files.deleteIfExists(resolve(objectKey));
        }
    }

    private Path resolve(String objectKey) throws IOException {
        Path target = root.resolve(objectKey).normalize();
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Path absoluteTarget = target.toAbsolutePath().normalize();
        if (!absoluteTarget.startsWith(normalizedRoot)) {
            throw new IOException("object key escapes remote object store root: " + objectKey);
        }
        return target;
    }
}
