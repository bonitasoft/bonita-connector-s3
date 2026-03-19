package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeneratePresignedUrlConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_HTTP_METHOD = "httpMethod";
    static final String INPUT_EXPIRATION_SECONDS = "expirationSeconds";
    static final String INPUT_CONTENT_TYPE = "contentType";
    static final String INPUT_VERSION_ID = "versionId";

    static final String OUTPUT_PRESIGNED_URL = "presignedUrl";
    static final String OUTPUT_EXPIRES_AT = "expiresAt";
    static final String OUTPUT_HTTP_METHOD = "httpMethod";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .objectKey(readStringInput(INPUT_OBJECT_KEY))
                .httpMethod(readStringInput(INPUT_HTTP_METHOD, "GET"))
                .expirationSeconds(readIntegerInput(INPUT_EXPIRATION_SECONDS, 3600))
                .contentType(readStringInput(INPUT_CONTENT_TYPE))
                .versionId(readStringInput(INPUT_VERSION_ID))
                .build();
    }

    @Override
    protected void validateConfiguration(S3Configuration config) {
        super.validateConfiguration(config);
        if (config.getBucketName() == null || config.getBucketName().isBlank()) {
            throw new IllegalArgumentException("bucketName is mandatory");
        }
        if (config.getObjectKey() == null || config.getObjectKey().isBlank()) {
            throw new IllegalArgumentException("objectKey is mandatory");
        }
        if (config.getHttpMethod() == null || config.getHttpMethod().isBlank()) {
            throw new IllegalArgumentException("httpMethod is mandatory");
        }
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Generating presigned {} URL for s3://{}/{}",
                configuration.getHttpMethod(), configuration.getBucketName(), configuration.getObjectKey());
        GeneratePresignedUrlResult result = client.generatePresignedUrl(configuration);
        setOutputParameter(OUTPUT_PRESIGNED_URL, result.presignedUrl());
        setOutputParameter(OUTPUT_EXPIRES_AT, result.expiresAt());
        setOutputParameter(OUTPUT_HTTP_METHOD, result.httpMethod());
        log.info("Presigned URL generated, expires at {}", result.expiresAt());
    }
}
