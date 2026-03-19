package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ListObjectsConnector extends AbstractS3Connector {

    static final String INPUT_BUCKET_NAME = "bucketName";
    static final String INPUT_PREFIX = "prefix";
    static final String INPUT_DELIMITER = "delimiter";
    static final String INPUT_MAX_KEYS = "maxKeys";
    static final String INPUT_CONTINUATION_TOKEN = "continuationToken";

    static final String OUTPUT_OBJECTS = "objects";
    static final String OUTPUT_COMMON_PREFIXES = "commonPrefixes";
    static final String OUTPUT_TOTAL_COUNT = "totalCount";
    static final String OUTPUT_IS_TRUNCATED = "isTruncated";
    static final String OUTPUT_NEXT_CONTINUATION_TOKEN = "nextContinuationToken";

    @Override
    protected S3Configuration buildConfiguration() {
        return connectionConfig()
                .bucketName(readStringInput(INPUT_BUCKET_NAME))
                .prefix(readStringInput(INPUT_PREFIX))
                .delimiter(readStringInput(INPUT_DELIMITER, "/"))
                .maxKeys(readIntegerInput(INPUT_MAX_KEYS, 1000))
                .continuationToken(readStringInput(INPUT_CONTINUATION_TOKEN))
                .build();
    }

    @Override
    protected void validateConfiguration(S3Configuration config) {
        super.validateConfiguration(config);
        if (config.getBucketName() == null || config.getBucketName().isBlank()) {
            throw new IllegalArgumentException("bucketName is mandatory");
        }
    }

    @Override
    protected void doExecute() throws S3Exception {
        log.info("Listing objects in s3://{}/{}", configuration.getBucketName(),
                configuration.getPrefix() != null ? configuration.getPrefix() : "");
        ListObjectsResult result = client.listObjects(configuration);
        setOutputParameter(OUTPUT_OBJECTS, result.objects());
        setOutputParameter(OUTPUT_COMMON_PREFIXES, result.commonPrefixes());
        setOutputParameter(OUTPUT_TOTAL_COUNT, result.totalCount());
        setOutputParameter(OUTPUT_IS_TRUNCATED, result.isTruncated());
        setOutputParameter(OUTPUT_NEXT_CONTINUATION_TOKEN, result.nextContinuationToken());
        log.info("Listed {} objects (truncated: {})", result.totalCount(), result.isTruncated());
    }
}
