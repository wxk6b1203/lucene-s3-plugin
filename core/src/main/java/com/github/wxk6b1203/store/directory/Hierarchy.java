package com.github.wxk6b1203.store.directory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Hierarchy {
    DATA("_data"),
    TEMP("_temp"),
    STATE("_state");
    @JsonValue
    public final String path;
    @JsonCreator
    public static Hierarchy of(String path) {
        for (Hierarchy hierarchy : values()) {
            if (hierarchy.path.equals(path)) {
                return hierarchy;
            }
        }
        throw new IllegalArgumentException("Unknown hierarchy path: " + path);
    }
}
