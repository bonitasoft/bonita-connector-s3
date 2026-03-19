package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CopyObjectConnector extends AbstractS3Connector {

    static final String INPUT_SOURCE_BUCKET_NAME = "sourceBucketName";
    static final String INPUT_SOURCE_OBJECT_KEY = "sourceObjectKey";
    static final String INPUT_SOURCE_VERSION_ID = "sourceVersionId";
    static final String INPUT_DESTINATION_BUCKET_NAME = "destinationBucketName";
    static final String INPUT_DESTINATION_OBJECT_KEY = "destinationObjectKey";
    static final String INPUT_STORAGE_CLASS = "storageClass";
    static final String INPUT_REPLACE_METADATA = "replaceMetadata";
    static final String INPUT_NEW_METADATA = "newMetadata";

    static final String OUTPUT_DESTINATION_OBJECT_KEY = "destinationObjectKey";
    static final String OUTPUT_DESTINATION_OBJECT_URL = "destinationObjectUrl";
    static final String OUTPUT_ETAG = "eTag";
    static final String OUTPUT_VERSION_ID = "versionId";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .sourceBucketName(readStringInput(INPUT_SOURCE_BUCKET_NAME))
                .sourceObjectKey(readStringInput(INPUT_SOURCE_OBJECT_KEY))
                .sourceVersionId(readStringInput(INPUT_SOURCE_VERSION_ID))
                .destinationBucketName(readStringInput(INPUT_DESTINATION_BUCKET_NAME))
                .destinationObjectKey(readStringInput(INPUT_DESTINATION_OBJECT_KEY))
                .storageClass(readStringInput(INPUT_STORAGE_CLASS))
                .replaceMetadata(readBooleanInput(INPUT_REPLACE_METADATA, false))
                .newMetadata(readMapInput(INPUT_NEW_METADATA))
                .build();
    }

    @Override
    protected void validateConfiguration(S3Configuration config) {
        super.validateConfiguration(config);
        if (config.getSourceBucketName() == null || config.getSourceBucketName().isBlank()) {
            throw new IllegalArgumentException("sourceBucketName is mandatory");
        }
        if (config.getSourceObjectKey() == null || config.getSourceObjectKey().isBlank()) {
            throw new IllegalArgumentException("sourceObjectKey is mandatory");
        }
        if (config.getDestinationBucketName() == null || config.getDestinationBucketName().isBlank()) {
            throw new IllegalArgumentException("destinationBucketName is mandatory");
        }
        if (config.getDestinationObjectKey() == null || config.getDestinationObjectKey().isBlank()) {
            throw new IllegalArgumentException("destinationObjectKey is mandatory");
        }
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Copying s3://{}/{} to s3://{}/{}",
                configuration.getSourceBucketName(), configuration.getSourceObjectKey(),
                configuration.getDestinationBucketName(), configuration.getDestinationObjectKey());
        CopyObjectResult result = client.copyObject(configuration);
        setOutputParameter(OUTPUT_DESTINATION_OBJECT_KEY, result.destinationObjectKey());
        setOutputParameter(OUTPUT_DESTINATION_OBJECT_URL, result.destinationObjectUrl());
        setOutputParameter(OUTPUT_ETAG, result.eTag());
        setOutputParameter(OUTPUT_VERSION_ID, result.versionId());
        log.info("Copy completed: {}", result.destinationObjectUrl());
    }
}
