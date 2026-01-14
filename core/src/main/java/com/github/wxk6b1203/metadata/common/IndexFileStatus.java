package com.github.wxk6b1203.metadata.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum IndexFileStatus {
    // WRITING: file is being written to WAL local storage
    WRITING(0),
    // DIRTY: the file is on WAL local storage but has not been uploaded to remote storage
    DIRTY(1),
    // UPLOADING: the file is being uploaded to remote storage
    UPLOADING(2),
    // CLEAN: the file has been uploaded to remote storage and has same content as local storage
    CLEAN(3),
    // PINNED: the file is pinned in local storage and will not be deleted
    PINNED(4),
    // DELETING: the file is being deleted from local and remote storage
    DELETING(5),
    // CLEANING: the file is being cleaned up from local storage
    CLEANING(6)
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
            case WRITING -> to == DIRTY || to == UPLOADING || to == DELETING;
            case DIRTY -> to == UPLOADING || to == DELETING;
            case UPLOADING -> to == CLEAN || to == DELETING;
            case CLEAN -> to == PINNED || to == DELETING || to == CLEANING;
            case PINNED -> to == DELETING;
            case DELETING -> to == CLEANING;
            default -> false;
        };
    }
}
