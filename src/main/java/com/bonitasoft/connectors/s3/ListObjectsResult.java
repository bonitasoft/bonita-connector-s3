package com.bonitasoft.connectors.s3;

import java.util.List;
import java.util.Map;

public record ListObjectsResult(
        List<Map<String, Object>> objects,
        List<String> commonPrefixes,
        Integer totalCount,
        Boolean isTruncated,
        String nextContinuationToken) {}
