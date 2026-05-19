package com.github.wxk6b1203.metadata.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum IndexFileStatus {
    // DIRTY: the file is on WAL local storage but has not been uploaded to remote storage
    DIRTY(0),
    // UPLOADING: the file is being uploaded to remote storage
    UPLOADING(1),
    // CLEAN: the file has been uploaded to remote storage and has same content as local storage
    CLEAN(2),
    // PINNED: the file is pinned in local storage and will not be deleted
    PINNED(3)
    ;

    @JsonValue
    public final int value;

    @JsonCreator
    public static IndexFileStatus fromValue(int value) {
        for (IndexFileStatus status : IndexFileStatus.values()) {
            if (status.value == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown IndexFileStatus value: " + value);
    }

    public static boolean validTransition(IndexFileStatus from, IndexFileStatus to) {
        if (from == to) {
            return true;
        }
        return switch (from) {
            case DIRTY -> to == UPLOADING;
            case UPLOADING -> to == CLEAN;
            case CLEAN -> to == PINNED;
            case PINNED -> false;
        };
    }
}
