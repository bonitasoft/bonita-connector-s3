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
class MultipartUploadConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private MultipartUploadConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new MultipartUploadConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-test-bucket");
        inputs.put("objectKey", "large-files/bigfile.zip");
        inputs.put("fileContentBase64", "SGVsbG8gTXVsdGlwYXJ0");
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
        when(mockClient.multipartUpload(any())).thenReturn(
                new MultipartUploadResult(
                        "https://my-test-bucket.s3.amazonaws.com/large-files/bigfile.zip",
                        "etag-mp-001", "v-mp-001", 3, 15728640L));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("objectUrl")).isNotNull();
        assertThat(outputs.get("totalPartsUploaded")).isEqualTo(3);
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
        when(mockClient.multipartUpload(any())).thenThrow(new S3Exception("Multipart upload failed: AbortedByClient"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("Multipart upload failed");
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
        assertThat(config.getPartSizeBytes()).isEqualTo(52428800L);
        assertThat(config.getContentType()).isEqualTo("application/octet-stream");
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.multipartUpload(any())).thenReturn(
                new MultipartUploadResult(
                        "https://bucket.s3.amazonaws.com/key.zip",
                        "etag-final", "v-final", 5, 52428800L));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("objectUrl")).isEqualTo("https://bucket.s3.amazonaws.com/key.zip");
        assertThat(outputs.get("eTag")).isEqualTo("etag-final");
        assertThat(outputs.get("versionId")).isEqualTo("v-final");
        assertThat(outputs.get("totalPartsUploaded")).isEqualTo(5);
        assertThat(outputs.get("totalBytesUploaded")).isEqualTo(52428800L);
    }

    @Test
    void shouldAcceptLocalFilePathInsteadOfBase64() throws Exception {
        inputs.remove("fileContentBase64");
        inputs.put("localFilePath", "/tmp/largefile.zip");
        connector.setInputParameters(inputs);
        assertThatCode(() -> connector.validateInputParameters()).doesNotThrowAnyException();
    }

    @Test
    void shouldFailValidationWhenNeitherFileContentNorLocalFilePath() {
        inputs.remove("fileContentBase64");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }
}
