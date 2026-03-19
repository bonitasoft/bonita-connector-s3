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
class DownloadObjectConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private DownloadObjectConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new DownloadObjectConnector();
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
        when(mockClient.downloadObject(any())).thenReturn(
                new DownloadObjectResult("SGVsbG8gV29ybGQ=", "application/pdf", 1024L,
                        "d41d8cd98f00b204e9800998ecf8427e", "2024-01-15T10:30:00Z", "ver-001",
                        Map.of("author", "test")));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("fileContentBase64")).isEqualTo("SGVsbG8gV29ybGQ=");
        assertThat(outputs.get("contentType")).isEqualTo("application/pdf");
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
        when(mockClient.downloadObject(any())).thenThrow(new S3Exception("NoSuchKey: The specified key does not exist"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("NoSuchKey");
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
        assertThat(config.getByteRangeStart()).isNull();
        assertThat(config.getByteRangeEnd()).isNull();
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.downloadObject(any())).thenReturn(
                new DownloadObjectResult("dGVzdA==", "text/plain", 4L,
                        "etag-abc", "2024-03-10T08:00:00Z", "v-999",
                        Map.of("custom-key", "custom-value")));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("fileContentBase64")).isEqualTo("dGVzdA==");
        assertThat(outputs.get("contentType")).isEqualTo("text/plain");
        assertThat(outputs.get("contentLengthBytes")).isEqualTo(4L);
        assertThat(outputs.get("eTag")).isEqualTo("etag-abc");
        assertThat(outputs.get("lastModified")).isEqualTo("2024-03-10T08:00:00Z");
        assertThat(outputs.get("versionId")).isEqualTo("v-999");
        assertThat(outputs.get("metadata")).isNotNull();
    }

    @Test
    void shouldHandleAccessDenied() throws Exception {
        injectMockClient();
        when(mockClient.downloadObject(any())).thenThrow(new S3Exception("Access Denied", 403, false));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("Access Denied");
    }

    @Test
    void shouldAcceptOptionalVersionId() throws Exception {
        inputs.put("versionId", "specific-version-id");
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getVersionId()).isEqualTo("specific-version-id");
    }
}
