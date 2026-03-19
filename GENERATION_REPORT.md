# Connector Generation Report

## Summary
- **Connector name:** s3
- **Display name:** AWS S3
- **Generated:** 2026-03-19
- **Operations:** 8
- **GitHub repo:** https://github.com/bonitasoft/bonita-connector-s3

## Operations Generated

| Operation | Def ID | Class | Unit Tests | Property Tests | Integration Test |
|---|---|---|---|---|---|
| Upload Object | s3-upload-object | UploadObjectConnector | 10 | 10 | Yes (skippable) |
| Download Object | s3-download-object | DownloadObjectConnector | 10 | 10 | Yes (skippable) |
| Delete Object | s3-delete-object | DeleteObjectConnector | 10 | 10 | Yes (skippable) |
| Copy Object | s3-copy-object | CopyObjectConnector | 10 | 10 | Yes (skippable) |
| Head Object | s3-head-object | HeadObjectConnector | 10 | 10 | Yes (skippable) |
| List Objects | s3-list-objects | ListObjectsConnector | 10 | 10 | Yes (skippable) |
| Generate Presigned URL | s3-generate-presigned-url | GeneratePresignedUrlConnector | 10 | 10 | Yes (skippable) |
| Multipart Upload | s3-multipart-upload | MultipartUploadConnector | 10 | 10 | Yes (skippable) |

## Test Summary

| Metric | Value |
|---|---|
| Total unit tests | 80 |
| Total property tests | 80 |
| Total integration tests | 8 (per-operation) + 8 (process-based IT) |
| Tests run | 168 |
| Failures | 0 |
| Skipped | 8 (integration tests, no AWS credentials set) |

## Build Artifacts

| Artifact | Description |
|---|---|
| `bonita-connector-s3-1.0.0.jar` | Main JAR for Bonita Studio import |
| `bonita-connector-s3-1.0.0-all.zip` | All 8 operations bundled |
| `bonita-connector-s3-1.0.0-upload-object-impl.zip` | Upload Object operation ZIP |
| `bonita-connector-s3-1.0.0-download-object-impl.zip` | Download Object operation ZIP |
| `bonita-connector-s3-1.0.0-delete-object-impl.zip` | Delete Object operation ZIP |
| `bonita-connector-s3-1.0.0-copy-object-impl.zip` | Copy Object operation ZIP |
| `bonita-connector-s3-1.0.0-head-object-impl.zip` | Head Object operation ZIP |
| `bonita-connector-s3-1.0.0-list-objects-impl.zip` | List Objects operation ZIP |
| `bonita-connector-s3-1.0.0-generate-presigned-url-impl.zip` | Generate Presigned URL operation ZIP |
| `bonita-connector-s3-1.0.0-multipart-upload-impl.zip` | Multipart Upload operation ZIP |

## Architecture

- **Single-module** Maven project (flat package `com.bonitasoft.connectors.s3`)
- **AWS SDK v2** (2.31.9): s3, s3-transfer-manager, presigner
- **Auth modes:** IAM static credentials, STS temporary credentials, default credential chain
- **Retry:** Exponential backoff with jitter (max 5 attempts) on 429/500/502/503/504
- **Error truncation:** All error messages truncated to 1000 chars

## Integration Test Environment Variables

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key (gates all integration tests) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_REGION` | AWS region (default: us-east-1) |
| `AWS_S3_TEST_BUCKET` | Test bucket name |

## Commands

| Action | Command |
|---|---|
| Build | `mvn clean verify` |
| Install | `mvn install -DskipTests` |
| Integration tests | `mvn verify` (after setting env vars) |
| Import to Bonita Studio | Use `bonita-connector-s3-1.0.0.jar` via "Import from file" |
