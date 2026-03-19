# CLAUDE.md

## Project Overview

Bonita AWS S3 connector — 8 operations for interacting with Amazon S3 from Bonita BPM processes.

**Technology Stack:**
- Java 17
- Maven (single-module)
- Bonita Runtime 10.2.0+
- AWS SDK v2 (s3, s3-transfer-manager, presigner)

## Operations

| Operation | Class | Description |
|---|---|---|
| s3-upload-object | UploadObjectConnector | Upload file via single PUT |
| s3-download-object | DownloadObjectConnector | Download object as Base64 |
| s3-delete-object | DeleteObjectConnector | Delete object (idempotent) |
| s3-copy-object | CopyObjectConnector | Copy within/across buckets |
| s3-head-object | HeadObjectConnector | Get metadata without download |
| s3-list-objects | ListObjectsConnector | List with pagination |
| s3-generate-presigned-url | GeneratePresignedUrlConnector | Time-limited GET/PUT URLs |
| s3-multipart-upload | MultipartUploadConnector | Large file upload via Transfer Manager |

## Build and Test Commands

```bash
# Build with tests
mvn clean verify

# Install to local repo (required for Bonita Studio import)
mvn install -DskipTests

# Run unit tests only
mvn test

# Run integration tests (requires AWS credentials)
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... mvn verify
```

## Architecture

- All classes in flat package `com.bonitasoft.connectors.s3`
- `AbstractS3Connector` — template method base class
- `S3ConnectorClient` — AWS SDK facade with retry policy
- `S3Configuration` — Lombok @Data @Builder config
- Per-operation: `{Op}Connector`, `{Op}Result` record, `.def`, `.impl`, `.properties`

## Commit Message Format

Use conventional commits: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`, `perf`

## Release Process

Releases are managed via GitHub Actions:
1. Run the "Release" workflow with the target version
2. Optionally set `update_marketplace: true` to publish to the Bonita marketplace
