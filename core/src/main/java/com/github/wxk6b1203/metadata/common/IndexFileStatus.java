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
    DELETING(5)
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
}
