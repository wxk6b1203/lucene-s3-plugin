package com.github.wxk6b1203.cluster;

public enum ShardState {
    UNASSIGNED,
    INITIALIZING,
    STARTED,
    RELOCATING,
    FAILED
}
