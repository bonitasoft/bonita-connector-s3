package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MultipartUploadConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_FILE_CONTENT_BASE64 = "fileContentBase64";
    static final String INPUT_LOCAL_FILE_PATH = "localFilePath";
    static final String INPUT_CONTENT_TYPE = "contentType";
    static final String INPUT_STORAGE_CLASS = "storageClass";
    static final String INPUT_PART_SIZE_BYTES = "partSizeBytes";
    static final String INPUT_METADATA = "metadata";
    static final String INPUT_SERVER_SIDE_ENCRYPTION = "serverSideEncryption";

    static final String OUTPUT_OBJECT_URL = "objectUrl";
    static final String OUTPUT_ETAG = "eTag";
    static final String OUTPUT_VERSION_ID = "versionId";
    static final String OUTPUT_TOTAL_PARTS_UPLOADED = "totalPartsUploaded";
    static final String OUTPUT_TOTAL_BYTES_UPLOADED = "totalBytesUploaded";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .objectKey(readStringInput(INPUT_OBJECT_KEY))
                .fileContentBase64(readStringInput(INPUT_FILE_CONTENT_BASE64))
                .localFilePath(readStringInput(INPUT_LOCAL_FILE_PATH))
                .contentType(readStringInput(INPUT_CONTENT_TYPE, "application/octet-stream"))
                .storageClass(readStringInput(INPUT_STORAGE_CLASS, "STANDARD"))
                .partSizeBytes(readLongInput(INPUT_PART_SIZE_BYTES, 52428800L))
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
        boolean hasBase64 = config.getFileContentBase64() != null && !config.getFileContentBase64().isBlank();
        boolean hasLocalFile = config.getLocalFilePath() != null && !config.getLocalFilePath().isBlank();
        if (!hasBase64 && !hasLocalFile) {
            throw new IllegalArgumentException("Either fileContentBase64 or localFilePath must be provided");
        }
        if (hasBase64 && hasLocalFile) {
            throw new IllegalArgumentException("Provide only one of fileContentBase64 or localFilePath, not both");
        }
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Multipart uploading to s3://{}/{}", configuration.getBucketName(), configuration.getObjectKey());
        MultipartUploadResult result = client.multipartUpload(configuration);
        setOutputParameter(OUTPUT_OBJECT_URL, result.objectUrl());
        setOutputParameter(OUTPUT_ETAG, result.eTag());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        setOutputParameter(OUTPUT_TOTAL_PARTS_UPLOADED, result.totalPartsUploaded());
        setOutputParameter(OUTPUT_TOTAL_BYTES_UPLOADED, result.totalBytesUploaded());
        log.info("Multipart upload completed: {} parts, {} bytes", result.totalPartsUploaded(), result.totalBytesUploaded());
    }
}
