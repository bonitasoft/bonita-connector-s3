package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeadObjectConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_VERSION_ID = "versionId";

    static final String OUTPUT_CONTENT_TYPE = "contentType";
    static final String OUTPUT_CONTENT_LENGTH_BYTES = "contentLengthBytes";
    static final String OUTPUT_ETAG = "eTag";
    static final String OUTPUT_LAST_MODIFIED = "lastModified";
    static final String OUTPUT_STORAGE_CLASS = "storageClass";
    static final String OUTPUT_VERSION_ID = "versionId";
    static final String OUTPUT_SERVER_SIDE_ENCRYPTION = "serverSideEncryption";
    static final String OUTPUT_METADATA = "metadata";
    static final String OUTPUT_OBJECT_URL = "objectUrl";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .objectKey(readStringInput(INPUT_OBJECT_KEY))
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
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Getting metadata for s3://{}/{}", configuration.getBucketName(), configuration.getObjectKey());
        HeadObjectResult result = client.headObject(configuration);
        setOutputParameter(OUTPUT_CONTENT_TYPE, result.contentType());
        setOutputParameter(OUTPUT_CONTENT_LENGTH_BYTES, result.contentLengthBytes());
        setOutputParameter(OUTPUT_ETAG, result.eTag());
        setOutputParameter(OUTPUT_LAST_MODIFIED, result.lastModified());
        setOutputParameter(OUTPUT_STORAGE_CLASS, result.storageClass());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        setOutputParameter(OUTPUT_SERVER_SIDE_ENCRYPTION, result.serverSideEncryption());
        setOutputParameter(OUTPUT_METADATA, result.metadata());
        setOutputParameter(OUTPUT_OBJECT_URL, result.objectUrl());
        log.info("Head object completed: {} ({} bytes)", result.contentType(), result.contentLengthBytes());
    }
}
