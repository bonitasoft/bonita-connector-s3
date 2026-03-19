package com.bonitasoft.connectors.s3;

import java.util.Map;

public record HeadObjectResult(
        String contentType,
        Long contentLengthBytes,
        String eTag,
        String lastModified,
        String storageClass,
        String versionId,
        String serverSideEncryption,
        Map<String, String> metadata,
        String objectUrl) {}
