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
    PINNED(3),
    // DELETING: the file is being deleted from local and remote storage
    DELETING(4),
    // CLEANING: the file is being cleaned up from remote storage
    CLEANING(5),
    DELETED(6)
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
        return switch (from) {
            // TODO: fix and add more transitions if needed
            case DIRTY -> to == UPLOADING || to == DELETING;
            case UPLOADING -> to == CLEAN || to == DELETING;
            case CLEAN -> to == PINNED || to == DELETING || to == CLEANING;
            case PINNED -> to == DELETING;
            case DELETING -> to == CLEANING;
            case CLEANING ->  to == DELETED;
            default -> false;
        };
    }
}
