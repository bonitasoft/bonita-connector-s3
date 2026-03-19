package com.bonitasoft.connectors.s3;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class HeadObjectConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private HeadObjectConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new HeadObjectConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-test-bucket");
        inputs.put("objectKey", "test-folder/document.pdf");
    }

    private void injectMockClient() throws Exception {
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var clientField = AbstractS3Connector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldExecuteSuccessfully() throws Exception {
        injectMockClient();
        when(mockClient.headObject(any())).thenReturn(
                new HeadObjectResult("application/pdf", 2048L, "etag-head-001",
                        "2024-06-15T14:30:00Z", "STANDARD", "v-head-001",
                        "AES256", Map.of(), "https://bucket.s3.amazonaws.com/test-folder/document.pdf"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("contentType")).isEqualTo("application/pdf");
        assertThat(outputs.get("contentLengthBytes")).isEqualTo(2048L);
    }

    @Test
    void shouldFailValidationWhenBucketNameMissing() {
        inputs.remove("bucketName");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailValidationWhenObjectKeyMissing() {
        inputs.remove("objectKey");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailValidationWhenRegionMissing() {
        inputs.remove("region");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailValidationWhenCredentialsMissingAndDefaultChainDisabled() {
        inputs.remove("accessKeyId");
        inputs.put("useDefaultCredentialChain", false);
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldHandleClientException() throws Exception {
        injectMockClient();
        when(mockClient.headObject(any())).thenThrow(new S3Exception("404 Not Found", 404, false));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("404");
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getConnectTimeout()).isEqualTo(30000);
        assertThat(config.getReadTimeout()).isEqualTo(60000);
        assertThat(config.getVersionId()).isNull();
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.headObject(any())).thenReturn(
                new HeadObjectResult("text/plain", 512L, "etag-abc",
                        "2024-01-01T00:00:00Z", "GLACIER", "v-123",
                        "aws:kms", Map.of("env", "prod"),
                        "https://bucket.s3.amazonaws.com/key"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("contentType")).isEqualTo("text/plain");
        assertThat(outputs.get("contentLengthBytes")).isEqualTo(512L);
        assertThat(outputs.get("eTag")).isEqualTo("etag-abc");
        assertThat(outputs.get("lastModified")).isEqualTo("2024-01-01T00:00:00Z");
        assertThat(outputs.get("storageClass")).isEqualTo("GLACIER");
        assertThat(outputs.get("versionId")).isEqualTo("v-123");
        assertThat(outputs.get("serverSideEncryption")).isEqualTo("aws:kms");
        assertThat(outputs.get("metadata")).isNotNull();
        assertThat(outputs.get("objectUrl")).isNotNull();
    }

    @Test
    void shouldHandleObjectNotFound() throws Exception {
        injectMockClient();
        when(mockClient.headObject(any())).thenThrow(new S3Exception("NoSuchKey: Object not found"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("NoSuchKey");
    }

    @Test
    void shouldAcceptOptionalVersionId() throws Exception {
        inputs.put("versionId", "head-version-id");
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getVersionId()).isEqualTo("head-version-id");
    }
}
