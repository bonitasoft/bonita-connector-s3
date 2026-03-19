package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class S3ConnectorClient {

    private final software.amazon.awssdk.services.s3.S3Client s3Client;
    private final S3Presigner presigner;
    private final S3TransferManager transferManager;
    private final RetryPolicy retryPolicy;
    private final S3Configuration configuration;

    public S3ConnectorClient(S3Configuration configuration) throws S3Exception {
        this.configuration = configuration;
        this.retryPolicy = new RetryPolicy(configuration.getMaxRetries());

        try {
            AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(configuration);
            Region region = Region.of(configuration.getRegion());

            this.s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(c -> c
                            .apiCallTimeout(Duration.ofMillis(configuration.getReadTimeout()))
                            .apiCallAttemptTimeout(Duration.ofMillis(configuration.getConnectTimeout())))
                    .build();

            this.presigner = S3Presigner.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .build();

            this.transferManager = S3TransferManager.builder()
                    .s3Client(software.amazon.awssdk.services.s3.S3AsyncClient.builder()
                            .region(region)
                            .credentialsProvider(credentialsProvider)
                            .build())
                    .build();

            log.debug("S3ConnectorClient initialized for region {}", configuration.getRegion());
        } catch (Exception e) {
            throw new S3Exception("Failed to initialize S3 client: " + e.getMessage(), e);
        }
    }

    public void close() {
        try { s3Client.close(); } catch (Exception e) { log.debug("Error closing S3Client", e); }
        try { presigner.close(); } catch (Exception e) { log.debug("Error closing S3Presigner", e); }
        try { transferManager.close(); } catch (Exception e) { log.debug("Error closing S3TransferManager", e); }
    }

    public UploadObjectResult uploadObject(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                byte[] content = Base64.getDecoder().decode(config.getFileContentBase64());
                if (content.length > 100 * 1024 * 1024) {
                    log.warn("File size ({} bytes) exceeds 100 MiB; consider s3-multipart-upload", content.length);
                }

                var reqBuilder = PutObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(config.getObjectKey())
                        .contentType(config.getContentType() != null ? config.getContentType() : "application/octet-stream");

                if (config.getStorageClass() != null && !config.getStorageClass().isBlank()) {
                    reqBuilder.storageClass(StorageClass.fromValue(config.getStorageClass()));
                }
                if (config.getServerSideEncryption() != null && !config.getServerSideEncryption().isBlank()) {
                    reqBuilder.serverSideEncryption(ServerSideEncryption.fromValue(config.getServerSideEncryption()));
                }
                if (config.getMetadata() != null) {
                    reqBuilder.metadata(config.getMetadata());
                }

                PutObjectResponse response = s3Client.putObject(reqBuilder.build(),
                        RequestBody.fromBytes(content));

                String objectUrl = buildObjectUrl(config.getBucketName(), config.getObjectKey());
                String versionId = response.versionId() != null ? response.versionId() : "";

                return new UploadObjectResult(objectUrl, response.eTag(), versionId);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public DownloadObjectResult downloadObject(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                var reqBuilder = GetObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(config.getObjectKey());

                if (config.getVersionId() != null && !config.getVersionId().isBlank()) {
                    reqBuilder.versionId(config.getVersionId());
                }
                if (config.getByteRangeStart() != null && config.getByteRangeEnd() != null) {
                    reqBuilder.range("bytes=" + config.getByteRangeStart() + "-" + config.getByteRangeEnd());
                }

                ResponseBytes<GetObjectResponse> responseBytes = s3Client.getObjectAsBytes(reqBuilder.build());
                GetObjectResponse response = responseBytes.response();

                String fileContentBase64 = Base64.getEncoder().encodeToString(responseBytes.asByteArray());
                String lastModified = response.lastModified() != null
                        ? response.lastModified().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        : "";
                String versionId = response.versionId() != null ? response.versionId() : "";
                Map<String, String> metadata = response.metadata() != null ? response.metadata() : Map.of();

                return new DownloadObjectResult(fileContentBase64, response.contentType(),
                        response.contentLength(), response.eTag(), lastModified, versionId, metadata);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public DeleteObjectResult deleteObject(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                log.warn("Deleting object s3://{}/{}", config.getBucketName(), config.getObjectKey());

                var reqBuilder = DeleteObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(config.getObjectKey());

                if (config.getVersionId() != null && !config.getVersionId().isBlank()) {
                    reqBuilder.versionId(config.getVersionId());
                }

                DeleteObjectResponse response = s3Client.deleteObject(reqBuilder.build());

                boolean deleteMarkerCreated = Boolean.TRUE.equals(response.deleteMarker());
                String versionId = response.versionId() != null ? response.versionId() : "";

                return new DeleteObjectResult(config.getObjectKey(), deleteMarkerCreated, versionId);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public CopyObjectResult copyObject(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                var reqBuilder = CopyObjectRequest.builder()
                        .sourceBucket(config.getSourceBucketName())
                        .sourceKey(config.getSourceObjectKey())
                        .destinationBucket(config.getDestinationBucketName())
                        .destinationKey(config.getDestinationObjectKey());

                if (config.getSourceVersionId() != null && !config.getSourceVersionId().isBlank()) {
                    reqBuilder.sourceVersionId(config.getSourceVersionId());
                }
                if (config.getStorageClass() != null && !config.getStorageClass().isBlank()) {
                    reqBuilder.storageClass(StorageClass.fromValue(config.getStorageClass()));
                }
                if (Boolean.TRUE.equals(config.getReplaceMetadata())) {
                    reqBuilder.metadataDirective(MetadataDirective.REPLACE);
                    if (config.getNewMetadata() != null) {
                        reqBuilder.metadata(config.getNewMetadata());
                    }
                } else {
                    reqBuilder.metadataDirective(MetadataDirective.COPY);
                }

                CopyObjectResponse response = s3Client.copyObject(reqBuilder.build());

                String destUrl = buildObjectUrl(config.getDestinationBucketName(), config.getDestinationObjectKey());
                String versionId = response.versionId() != null ? response.versionId() : "";

                return new CopyObjectResult(config.getDestinationObjectKey(), destUrl,
                        response.copyObjectResult().eTag(), versionId);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public HeadObjectResult headObject(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                var reqBuilder = HeadObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(config.getObjectKey());

                if (config.getVersionId() != null && !config.getVersionId().isBlank()) {
                    reqBuilder.versionId(config.getVersionId());
                }

                HeadObjectResponse response = s3Client.headObject(reqBuilder.build());

                String lastModified = response.lastModified() != null
                        ? response.lastModified().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        : "";
                String versionId = response.versionId() != null ? response.versionId() : "";
                String storageClass = response.storageClassAsString() != null ? response.storageClassAsString() : "STANDARD";
                String sse = response.serverSideEncryptionAsString() != null ? response.serverSideEncryptionAsString() : "";
                Map<String, String> metadata = response.metadata() != null ? response.metadata() : Map.of();
                String objectUrl = buildObjectUrl(config.getBucketName(), config.getObjectKey());

                return new HeadObjectResult(response.contentType(), response.contentLength(),
                        response.eTag(), lastModified, storageClass, versionId, sse, metadata, objectUrl);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public ListObjectsResult listObjects(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                var reqBuilder = ListObjectsV2Request.builder()
                        .bucket(config.getBucketName());

                if (config.getPrefix() != null && !config.getPrefix().isBlank()) {
                    reqBuilder.prefix(config.getPrefix());
                }
                if (config.getDelimiter() != null) {
                    reqBuilder.delimiter(config.getDelimiter());
                }
                if (config.getMaxKeys() != null) {
                    reqBuilder.maxKeys(config.getMaxKeys());
                }
                if (config.getContinuationToken() != null && !config.getContinuationToken().isBlank()) {
                    reqBuilder.continuationToken(config.getContinuationToken());
                }

                ListObjectsV2Response response = s3Client.listObjectsV2(reqBuilder.build());

                List<Map<String, Object>> objects = new ArrayList<>();
                for (S3Object obj : response.contents()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("key", obj.key());
                    item.put("size", obj.size());
                    item.put("lastModified", obj.lastModified() != null
                            ? obj.lastModified().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                            : "");
                    item.put("eTag", obj.eTag());
                    item.put("storageClass", obj.storageClassAsString());
                    objects.add(item);
                }

                List<String> commonPrefixes = response.commonPrefixes().stream()
                        .map(cp -> cp.prefix())
                        .toList();

                String nextToken = response.nextContinuationToken() != null
                        ? response.nextContinuationToken() : "";

                return new ListObjectsResult(objects, commonPrefixes,
                        response.keyCount(), response.isTruncated(), nextToken);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public GeneratePresignedUrlResult generatePresignedUrl(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                Duration expiration = Duration.ofSeconds(
                        config.getExpirationSeconds() != null ? config.getExpirationSeconds() : 3600);
                Instant expiresAt = Instant.now().plus(expiration);

                String url;
                if ("PUT".equalsIgnoreCase(config.getHttpMethod())) {
                    var putReqBuilder = PutObjectRequest.builder()
                            .bucket(config.getBucketName())
                            .key(config.getObjectKey());
                    if (config.getContentType() != null && !config.getContentType().isBlank()) {
                        putReqBuilder.contentType(config.getContentType());
                    }
                    var presignReq = PutObjectPresignRequest.builder()
                            .signatureDuration(expiration)
                            .putObjectRequest(putReqBuilder.build())
                            .build();
                    url = presigner.presignPutObject(presignReq).url().toString();
                } else {
                    var getReqBuilder = GetObjectRequest.builder()
                            .bucket(config.getBucketName())
                            .key(config.getObjectKey());
                    if (config.getVersionId() != null && !config.getVersionId().isBlank()) {
                        getReqBuilder.versionId(config.getVersionId());
                    }
                    var presignReq = GetObjectPresignRequest.builder()
                            .signatureDuration(expiration)
                            .getObjectRequest(getReqBuilder.build())
                            .build();
                    url = presigner.presignGetObject(presignReq).url().toString();
                }

                String expiresAtStr = expiresAt.atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

                return new GeneratePresignedUrlResult(url, expiresAtStr,
                        config.getHttpMethod() != null ? config.getHttpMethod().toUpperCase() : "GET");
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            }
        });
    }

    public MultipartUploadResult multipartUpload(S3Configuration config) throws S3Exception {
        return retryPolicy.execute(() -> {
            try {
                byte[] content;
                long totalBytes;

                if (config.getLocalFilePath() != null && !config.getLocalFilePath().isBlank()) {
                    Path filePath = Path.of(config.getLocalFilePath());
                    content = Files.readAllBytes(filePath);
                    totalBytes = content.length;
                } else if (config.getFileContentBase64() != null && !config.getFileContentBase64().isBlank()) {
                    content = Base64.getDecoder().decode(config.getFileContentBase64());
                    totalBytes = content.length;
                } else {
                    throw new S3Exception("Either fileContentBase64 or localFilePath must be provided");
                }

                if (totalBytes < 5 * 1024 * 1024) {
                    log.warn("File size ({} bytes) < 5 MiB; single PUT would be more efficient", totalBytes);
                }

                var putReqBuilder = PutObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(config.getObjectKey())
                        .contentType(config.getContentType() != null ? config.getContentType() : "application/octet-stream");

                if (config.getStorageClass() != null && !config.getStorageClass().isBlank()) {
                    putReqBuilder.storageClass(StorageClass.fromValue(config.getStorageClass()));
                }
                if (config.getServerSideEncryption() != null && !config.getServerSideEncryption().isBlank()) {
                    putReqBuilder.serverSideEncryption(ServerSideEncryption.fromValue(config.getServerSideEncryption()));
                }
                if (config.getMetadata() != null) {
                    putReqBuilder.metadata(config.getMetadata());
                }

                Upload upload = transferManager.upload(UploadRequest.builder()
                        .putObjectRequest(putReqBuilder.build())
                        .requestBody(AsyncRequestBody.fromBytes(content))
                        .build());

                CompletedUpload completedUpload = upload.completionFuture().join();
                var response = completedUpload.response();

                String objectUrl = buildObjectUrl(config.getBucketName(), config.getObjectKey());
                String versionId = response.versionId() != null ? response.versionId() : "";
                long partSize = config.getPartSizeBytes() != null ? config.getPartSizeBytes() : 52428800L;
                int totalParts = (int) Math.ceil((double) totalBytes / partSize);

                return new MultipartUploadResult(objectUrl, response.eTag(), versionId,
                        totalParts, totalBytes);
            } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
                throw mapAwsException(e);
            } catch (IOException e) {
                throw new S3Exception("Failed to read file: " + e.getMessage(), e);
            }
        });
    }

    private String buildObjectUrl(String bucket, String key) {
        return String.format("https://%s.s3.%s.amazonaws.com/%s",
                bucket, configuration.getRegion(), key);
    }

    private S3Exception mapAwsException(software.amazon.awssdk.services.s3.model.S3Exception e) {
        int statusCode = e.statusCode();
        boolean retryable = RetryPolicy.isRetryableStatusCode(statusCode);
        String message = truncate(e.getMessage());

        if (statusCode == 403) {
            log.error("Access denied (403) — check IAM policy. {}", message);
        } else if (statusCode == 404) {
            log.error("Not found (404) — {}", message);
        }

        return new S3Exception(message, statusCode, retryable, e);
    }

    private AwsCredentialsProvider buildCredentialsProvider(S3Configuration config) {
        if (Boolean.TRUE.equals(config.getUseDefaultCredentialChain())) {
            log.info("Using default AWS credential chain");
            return DefaultCredentialsProvider.create();
        }
        if (config.getSessionToken() != null && !config.getSessionToken().isBlank()) {
            log.debug("Using temporary STS credentials");
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(
                            config.getAccessKeyId(),
                            config.getSecretAccessKey(),
                            config.getSessionToken()));
        }
        log.debug("Using static IAM credentials");
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey()));
    }

    private String truncate(String message) {
        if (message == null) return "";
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }
}
