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
mvn clean verify          # Build with tests
mvn install -DskipTests   # Install to local repo (required for Bonita Studio import)
mvn test                  # Unit tests only
```

## Architecture

- All classes in flat package `com.bonitasoft.connectors.s3`
- `AbstractS3Connector` — template method base class (validates, connects, executes, disconnects)
- `S3ConnectorClient` — AWS SDK facade with retry policy
- `S3Configuration` — Lombok @Data @Builder config holding all parameters
- Per-operation: `{Op}Connector` (extends abstract), `{Op}Result` (Java record), `.def`, `.impl`, `.properties`

## Commit Message Format

Use conventional commits: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`, `perf`

## Release Process

Releases are managed via GitHub Actions. Run the "Release" workflow with the target version.

---

## Code Review Guidelines

**When reviewing PRs on this repository, use the checklists below exhaustively. Flag every violation. Do not skip sections.**

### Security — AWS & Connector-Specific

- [ ] **No hardcoded credentials.** No AWS access keys, secret keys, session tokens, or ARNs anywhere in source code, test code, or resource files. Credentials must come only from connector input parameters or environment variables.
- [ ] **No secret logging.** `accessKeyId`, `secretAccessKey`, `sessionToken` must NEVER appear in log output — not even at DEBUG level. Check all `log.*()` calls touching auth-related variables.
- [ ] **Error messages don't leak internals.** Error messages returned to the user (via `errorMessage` output) must not expose AWS account IDs, ARNs, internal hostnames, or full stack traces. All error messages must be truncated to 1000 characters.
- [ ] **Input validation at entry.** All user-supplied parameters must be validated before reaching the AWS SDK: bucket names, object keys (max 1024 bytes UTF-8), metadata (max 2 KB), `partSizeBytes` (min 5 MiB), `expirationSeconds` (max 604800).
- [ ] **No S3 key injection.** Object keys constructed from user input must not allow path traversal (`../`) or null bytes.
- [ ] **No SSRF vectors.** User-controlled strings must not be used to construct URLs that the server fetches (e.g., `localFilePath` must only read from the local filesystem, not URLs).
- [ ] **Presigned URL safety.** `contentType` binding on PUT presigned URLs must be documented/enforced. Expiration must be capped.
- [ ] **Resource cleanup.** `S3Client`, `S3Presigner`, and `S3TransferManager` must be closed in `disconnect()`. Verify no resource leaks on exception paths.
- [ ] **Dependency scope.** All non-test dependencies must be `provided` scope. No `compile` or `runtime` scope (causes Bonita Studio import failures).

### Code Quality — Bonita Connector Conventions

- [ ] **Outputs always set.** Every execution path (success AND failure) must set `success` and `errorMessage`. Check both the `doExecute()` happy path and the `catch` blocks in `executeBusinessLogic()`.
- [ ] **No `System.out.println`.** All logging via `@Slf4j` only (`log.info/debug/warn/error`).
- [ ] **No raw `Exception` catches.** Catch specific exceptions (`S3Exception`, `software.amazon.awssdk.services.s3.model.S3Exception`), wrap in project `S3Exception`. The only catch-all is in `executeBusinessLogic()` at the abstract level.
- [ ] **Retry policy correctness.** Only HTTP 429, 500, 502, 503, 504 are retryable. 400/401/403/404 must fail immediately. Verify `isRetryable()` flag is set correctly when mapping AWS exceptions.
- [ ] **Version is release.** POM version must be `X.Y.Z` (e.g., `1.0.0`), NEVER `-SNAPSHOT`. SNAPSHOT versions cause Bonita Studio import failures.
- [ ] **Java 17 features used appropriately.** Records for model objects, text blocks where helpful. No records for mutable state.
- [ ] **`.impl` file correctness.** Namespace must be `6.0` (not `6.1`). Element order must match XSD: `implementationId → implementationVersion → definitionId → definitionVersion → implementationClassname → hasSources → description → jarDependencies`. `<hasSources>false</hasSources>` must be present.
- [ ] **`.def` file correctness.** Namespace must be `6.1`. Widget IDs follow `{inputName}Widget` convention. All inputs have correct `type` and `mandatory` attributes.
- [ ] **`.properties` completeness.** Every widget in `.def` must have a corresponding `{widgetId}.label` and `{widgetId}.description` entry in `.properties`.
- [ ] **Flat package.** All Java classes in `com.bonitasoft.connectors.s3` — no sub-packages.

### Test Quality

- [ ] **No stub tests.** Every `@Test` method must contain assertions. No empty methods, no `// TODO` placeholders, no `assertTrue(true)`.
- [ ] **Happy path coverage.** Each operation must have a test that mocks the client to return a successful result and verifies ALL output parameters are set correctly.
- [ ] **Mandatory input validation tests.** Each mandatory input parameter must have a dedicated test that removes/blanks it and asserts `ConnectorValidationException` is thrown.
- [ ] **Error path coverage.** Each operation must have a test that mocks the client to throw `S3Exception` and verifies `success=false` and `errorMessage` is set (not null, not empty).
- [ ] **Mock injection correctness.** Tests must inject mocks via reflection on `AbstractS3Connector.class.getDeclaredField("client")`, not by subclassing or other patterns that bypass the production code path.
- [ ] **Property tests are meaningful.** Each `@Property` method must test a real property of the system (e.g., "blank mandatory inputs always cause validation failure", "valid configs always build without exception"). No trivial properties like `assertTrue(true)`.
- [ ] **Integration tests gated.** All integration tests must use `@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")`. They must NEVER run without explicit opt-in.
- [ ] **No test interdependencies.** Each test must be independent — no shared mutable state, no execution order assumptions.
- [ ] **Assertion quality.** Use AssertJ fluent assertions. Prefer specific assertions (`isEqualTo`, `isNotNull`, `hasSize`) over generic ones (`isNotNull` alone is insufficient when the value can be verified).
- [ ] **Edge cases tested.** At minimum: null optional inputs, empty strings, boundary values (e.g., 0-byte file, 1024-byte key, 2 KB metadata).
