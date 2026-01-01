package com.github.wxk6b1203.errors;

public interface Errors {
    Error INDEX_NOT_FOUND = new Error(
            "INDEX_NOT_FOUND",
            "The specified index was not found."
    );

    Error INDEX_ALREADY_EXISTS = new Error(
            "INDEX_ALREADY_EXISTS",
            "The specified index already exists."
    );

    Error STORAGE_FAILURE = new Error(
            "STORAGE_FAILURE",
            "An error occurred while accessing the storage."
    );
}
