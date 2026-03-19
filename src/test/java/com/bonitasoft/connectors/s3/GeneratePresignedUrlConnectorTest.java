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
class GeneratePresignedUrlConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private GeneratePresignedUrlConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new GeneratePresignedUrlConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-test-bucket");
        inputs.put("objectKey", "test-folder/document.pdf");
        inputs.put("httpMethod", "GET");
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
        when(mockClient.generatePresignedUrl(any())).thenReturn(
                new GeneratePresignedUrlResult(
                        "https://my-test-bucket.s3.amazonaws.com/test-folder/document.pdf?X-Amz-Signature=abc",
                        "2024-12-31T23:59:59Z", "GET"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat((String) outputs.get("presignedUrl")).contains("X-Amz-Signature");
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
        when(mockClient.generatePresignedUrl(any())).thenThrow(new S3Exception("Failed to generate presigned URL"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("presigned URL");
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
        assertThat(config.getExpirationSeconds()).isEqualTo(3600);
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.generatePresignedUrl(any())).thenReturn(
                new GeneratePresignedUrlResult(
                        "https://bucket.s3.amazonaws.com/key?signed",
                        "2024-06-01T12:00:00Z", "PUT"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("presignedUrl")).isNotNull();
        assertThat(outputs.get("expiresAt")).isEqualTo("2024-06-01T12:00:00Z");
        assertThat(outputs.get("httpMethod")).isEqualTo("PUT");
    }

    @Test
    void shouldDefaultHttpMethodToGet() throws Exception {
        inputs.remove("httpMethod");
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getHttpMethod()).isEqualTo("GET");
    }

    @Test
    void shouldAcceptPutHttpMethod() throws Exception {
        inputs.put("httpMethod", "PUT");
        connector.setInputParameters(inputs);
        assertThatCode(() -> connector.validateInputParameters()).doesNotThrowAnyException();
    }
}
