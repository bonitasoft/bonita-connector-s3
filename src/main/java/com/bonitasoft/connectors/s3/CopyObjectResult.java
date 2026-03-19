package com.bonitasoft.connectors.s3;

public record CopyObjectResult(String destinationObjectKey, String destinationObjectUrl, String eTag, String versionId) {}
