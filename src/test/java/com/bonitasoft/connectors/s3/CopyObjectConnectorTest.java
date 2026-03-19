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
class CopyObjectConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private CopyObjectConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new CopyObjectConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("sourceBucketName", "source-bucket");
        inputs.put("sourceObjectKey", "source-folder/original.pdf");
        inputs.put("destinationBucketName", "dest-bucket");
        inputs.put("destinationObjectKey", "dest-folder/copy.pdf");
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
        when(mockClient.copyObject(any())).thenReturn(
                new CopyObjectResult("dest-folder/copy.pdf",
                        "https://dest-bucket.s3.amazonaws.com/dest-folder/copy.pdf",
                        "etag-copy", "v-copy-001"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("destinationObjectKey")).isEqualTo("dest-folder/copy.pdf");
        assertThat(outputs.get("destinationObjectUrl")).isNotNull();
    }

    @Test
    void shouldFailValidationWhenSourceBucketNameMissing() {
        inputs.remove("sourceBucketName");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailValidationWhenSourceObjectKeyMissing() {
        inputs.remove("sourceObjectKey");
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
        when(mockClient.copyObject(any())).thenThrow(new S3Exception("NoSuchKey: Source object not found"));
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
        assertThat(config.getSourceVersionId()).isNull();
        assertThat(config.getReplaceMetadata()).isEqualTo(false);
    }

    @Test
    void shouldPopulateAllOutputFields() throws Exception {
        injectMockClient();
        when(mockClient.copyObject(any())).thenReturn(
                new CopyObjectResult("dest/key.txt",
                        "https://dest.s3.amazonaws.com/dest/key.txt",
                        "etag-xyz", "v-789"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("destinationObjectKey")).isEqualTo("dest/key.txt");
        assertThat(outputs.get("destinationObjectUrl")).isNotNull();
        assertThat(outputs.get("eTag")).isEqualTo("etag-xyz");
        assertThat(outputs.get("versionId")).isEqualTo("v-789");
    }

    @Test
    void shouldFailValidationWhenDestinationBucketNameMissing() {
        inputs.remove("destinationBucketName");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailValidationWhenDestinationObjectKeyMissing() {
        inputs.remove("destinationObjectKey");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }
}
