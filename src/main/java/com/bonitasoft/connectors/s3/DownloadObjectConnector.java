package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadObjectConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_VERSION_ID = "versionId";
    static final String INPUT_BYTE_RANGE_START = "byteRangeStart";
    static final String INPUT_BYTE_RANGE_END = "byteRangeEnd";

    static final String OUTPUT_FILE_CONTENT_BASE64 = "fileContentBase64";
    static final String OUTPUT_CONTENT_TYPE = "contentType";
    static final String OUTPUT_CONTENT_LENGTH_BYTES = "contentLengthBytes";
    static final String OUTPUT_ETAG = "eTag";
    static final String OUTPUT_LAST_MODIFIED = "lastModified";
    static final String OUTPUT_VERSION_ID = "versionId";
    static final String OUTPUT_METADATA = "metadata";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .objectKey(readStringInput(INPUT_OBJECT_KEY))
                .versionId(readStringInput(INPUT_VERSION_ID))
                .byteRangeStart(readLongInput(INPUT_BYTE_RANGE_START))
                .byteRangeEnd(readLongInput(INPUT_BYTE_RANGE_END))
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
        log.info("Downloading object from s3://{}/{}", configuration.getBucketName(), configuration.getObjectKey());
        DownloadObjectResult result = client.downloadObject(configuration);
        setOutputParameter(OUTPUT_FILE_CONTENT_BASE64, result.fileContentBase64());
        setOutputParameter(OUTPUT_CONTENT_TYPE, result.contentType());
        setOutputParameter(OUTPUT_CONTENT_LENGTH_BYTES, result.contentLengthBytes());
        setOutputParameter(OUTPUT_ETAG, result.eTag());
        setOutputParameter(OUTPUT_LAST_MODIFIED, result.lastModified());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        setOutputParameter(OUTPUT_METADATA, result.metadata());
        log.info("Download completed: {} bytes", result.contentLengthBytes());
    }
}
