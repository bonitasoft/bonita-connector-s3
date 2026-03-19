package com.bonitasoft.connectors.s3;

public record GeneratePresignedUrlResult(String presignedUrl, String expiresAt, String httpMethod) {}
