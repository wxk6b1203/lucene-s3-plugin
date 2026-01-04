package com.github.wxk6b1203.store.directory;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Hierarchy {
    DATA("_data"),
    STATE("_state");
    public final String path;
}
