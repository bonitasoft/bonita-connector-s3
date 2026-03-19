package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadObjectConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_FILE_CONTENT_BASE64 = "fileContentBase64";
    static final String INPUT_CONTENT_TYPE = "contentType";
    static final String INPUT_STORAGE_CLASS = "storageClass";
    static final String INPUT_METADATA = "metadata";
    static final String INPUT_SERVER_SIDE_ENCRYPTION = "serverSideEncryption";

    static final String OUTPUT_OBJECT_URL = "objectUrl";
    static final String OUTPUT_ETAG = "eTag";
    static final String OUTPUT_VERSION_ID = "versionId";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .objectKey(readStringInput(INPUT_OBJECT_KEY))
                .fileContentBase64(readStringInput(INPUT_FILE_CONTENT_BASE64))
                .contentType(readStringInput(INPUT_CONTENT_TYPE, "application/octet-stream"))
                .storageClass(readStringInput(INPUT_STORAGE_CLASS, "STANDARD"))
                .metadata(readMapInput(INPUT_METADATA))
                .serverSideEncryption(readStringInput(INPUT_SERVER_SIDE_ENCRYPTION))
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
        if (config.getFileContentBase64() == null || config.getFileContentBase64().isBlank()) {
            throw new IllegalArgumentException("fileContentBase64 is mandatory");
        }
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Uploading object to s3://{}/{}", configuration.getBucketName(), configuration.getObjectKey());
        UploadObjectResult result = client.uploadObject(configuration);
        setOutputParameter(OUTPUT_OBJECT_URL, result.objectUrl());
        setOutputParameter(OUTPUT_ETAG, result.eTag());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        log.info("Upload completed: {}", result.objectUrl());
    }
}
