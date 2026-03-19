package com.bonitasoft.connectors.s3;

public record MultipartUploadResult(
        String objectUrl,
        String eTag,
        String versionId,
        Integer totalPartsUploaded,
        Long totalBytesUploaded) {}
