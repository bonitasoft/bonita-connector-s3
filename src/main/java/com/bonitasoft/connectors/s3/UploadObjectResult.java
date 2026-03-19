package com.bonitasoft.connectors.s3;

public record UploadObjectResult(String objectUrl, String eTag, String versionId) {}
