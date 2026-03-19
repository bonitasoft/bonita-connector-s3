package com.bonitasoft.connectors.s3;

import java.util.Map;

public record DownloadObjectResult(
        String fileContentBase64,
        String contentType,
        Long contentLengthBytes,
        String eTag,
        String lastModified,
        String versionId,
        Map<String, String> metadata) {}
