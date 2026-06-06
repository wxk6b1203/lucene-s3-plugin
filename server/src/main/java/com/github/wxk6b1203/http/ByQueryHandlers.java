package com.github.wxk6b1203.http;

import com.github.wxk6b1203.search.ByQueryRequest;

import java.util.HashMap;
import java.util.Map;

final class ByQueryHandlers {
    private ByQueryHandlers() {
    }

    static Map<String, Object> byQueryBody(ByQueryRequest request) {
        Map<String, Object> body = new HashMap<>();
        body.put("query", request.query());
        body.put("doc", request.document());
        body.put("conflicts_proceed", request.conflictsProceed());
        body.put("mappings", request.mappings());
        if (request.ownerTerm() != null) {
            body.put("owner_term", request.ownerTerm());
        }
        if (request.allocationEpoch() != null) {
            body.put("allocation_epoch", request.allocationEpoch());
        }
        if (request.routing() != null) {
            body.put("routing", request.routing());
        }
        return body;
    }

    static long deletedCount(String status) {
        return count(status, "deleted=");
    }

    static long count(String status, String prefix) {
        if (status == null || !status.startsWith(prefix)) {
            return 0;
        }
        return Long.parseLong(status.substring(prefix.length()));
    }
}
