package com.bonitasoft.connectors.s3;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class S3Configuration {

    // Connection / Auth (Project/Runtime scope)
    private String accessKeyId;
    private String secretAccessKey;
    private String sessionToken;
    private String region;
    @Builder.Default
    private Boolean useDefaultCredentialChain = false;
    @Builder.Default
    private int connectTimeout = 30000;
    @Builder.Default
    private int readTimeout = 60000;
    @Builder.Default
    private int maxRetries = 5;

    // Common operation params
    private String bucketName;
    private String objectKey;
    private String fileContentBase64;
    private String contentType;
    private String storageClass;
    private Map<String, String> metadata;
    private String serverSideEncryption;
    private String versionId;

    // List Objects
    private String prefix;
    private String delimiter;
    private Integer maxKeys;
    private String continuationToken;

    // Copy Object
    private String sourceBucketName;
    private String sourceObjectKey;
    private String sourceVersionId;
    private String destinationBucketName;
    private String destinationObjectKey;
    private Boolean replaceMetadata;
    private Map<String, String> newMetadata;

    // Presigned URL
    private String httpMethod;
    private Integer expirationSeconds;

    // Download
    private Long byteRangeStart;
    private Long byteRangeEnd;

    // Multipart
    private String localFilePath;
    private Long partSizeBytes;
}
