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
class UploadObjectConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private UploadObjectConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new UploadObjectConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-test-bucket");
        inputs.put("objectKey", "test-folder/document.pdf");
        inputs.put("fileContentBase64", "SGVsbG8gV29ybGQ=");
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
        when(mockClient.uploadObject(any())).thenReturn(
                new UploadObjectResult("https://my-test-bucket.s3.amazonaws.com/test-folder/document.pdf",
                        "d41d8cd98f00b204e9800998ecf8427e", "ver-001"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("objectUrl")).isEqualTo("https://my-test-bucket.s3.amazonaws.com/test-folder/document.pdf");
        assertThat(outputs.get("eTag")).isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
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
        when(mockClient.uploadObject(any())).thenThrow(new S3Exception("NoSuchBucket: The specified bucket does not exist"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("NoSuchBucket");
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        var minimalInputs = new HashMap<String, Object>();
        minimalInputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        minimalInputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        minimalInputs.put("region", "us-east-1");
        minimalInputs.put("bucketName", "my-bucket");
        minimalInputs.put("objectKey", "test.txt");
        minimalInputs.put("fileContentBase64", "SGVsbG8=");
        connector.setInputParameters(minimalInputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getConnectTimeout()).isEqualTo(30000);
        assertThat(config.getReadTimeout()).isEqualTo(60000);
        assertThat(config.getMaxRetries()).isEqualTo(5);
        assertThat(config.getUseDefaultCredentialChain()).isFalse();
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.uploadObject(any())).thenReturn(
                new UploadObjectResult("https://bucket.s3.amazonaws.com/key", "etag-123", "v-456"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("objectUrl")).isNotNull();
        assertThat(outputs.get("eTag")).isNotNull();
        assertThat(outputs.get("versionId")).isEqualTo("v-456");
    }

    @Test
    void shouldFailValidationWhenFileContentBase64Missing() {
        inputs.remove("fileContentBase64");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldHandleUnexpectedException() throws Exception {
        injectMockClient();
        when(mockClient.uploadObject(any())).thenThrow(new RuntimeException("Unexpected network failure"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("Unexpected");
    }
}
