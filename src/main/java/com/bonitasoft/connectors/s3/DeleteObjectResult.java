package com.bonitasoft.connectors.s3;

public record DeleteObjectResult(String deletedObjectKey, Boolean deleteMarkerCreated, String versionId) {}
