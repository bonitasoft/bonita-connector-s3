package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteObjectConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_OBJECT_KEY = "objectKey";
    static final String INPUT_VERSION_ID = "versionId";

    static final String OUTPUT_DELETED_OBJECT_KEY = "deletedObjectKey";
    static final String OUTPUT_DELETE_MARKER_CREATED = "deleteMarkerCreated";
    static final String OUTPUT_VERSION_ID = "versionId";

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
        log.info("Deleting object s3://{}/{}", configuration.getBucketName(), configuration.getObjectKey());
        DeleteObjectResult result = client.deleteObject(configuration);
        setOutputParameter(OUTPUT_DELETED_OBJECT_KEY, result.deletedObjectKey());
        setOutputParameter(OUTPUT_DELETE_MARKER_CREATED, result.deleteMarkerCreated());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        log.info("Delete completed for: {}", result.deletedObjectKey());
    }
}
