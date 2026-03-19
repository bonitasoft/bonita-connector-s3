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
class DeleteObjectConnectorTest {

    @Mock
    private S3ConnectorClient mockClient;

    private DeleteObjectConnector connector;
    private Map<String, Object> inputs;

    @BeforeEach
    void setUp() {
        connector = new DeleteObjectConnector();
        inputs = new HashMap<>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-test-bucket");
        inputs.put("objectKey", "test-folder/to-delete.txt");
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
        when(mockClient.deleteObject(any())).thenReturn(
                new DeleteObjectResult("test-folder/to-delete.txt", false, null));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("deletedObjectKey")).isEqualTo("test-folder/to-delete.txt");
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
        when(mockClient.deleteObject(any())).thenThrow(new S3Exception("AccessDenied: Access Denied"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(false);
        assertThat((String) outputs.get("errorMessage")).contains("AccessDenied");
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
        when(mockClient.deleteObject(any())).thenReturn(
                new DeleteObjectResult("key.txt", true, "delete-marker-v1"));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
        assertThat(outputs.get("deletedObjectKey")).isEqualTo("key.txt");
        assertThat(outputs.get("deleteMarkerCreated")).isEqualTo(true);
        assertThat(outputs.get("versionId")).isEqualTo("delete-marker-v1");
    }

    @Test
    void shouldHandleDeleteOfNonExistentObject() throws Exception {
        injectMockClient();
        // S3 delete of non-existent object is idempotent -- returns success
        when(mockClient.deleteObject(any())).thenReturn(
                new DeleteObjectResult("nonexistent.txt", false, null));
        connector.executeBusinessLogic();
        var outputs = connector.getOutputs();
        assertThat(outputs.get("success")).isEqualTo(true);
    }

    @Test
    void shouldAcceptOptionalVersionId() throws Exception {
        inputs.put("versionId", "specific-version-to-delete");
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        var configField = AbstractS3Connector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (S3Configuration) configField.get(connector);
        assertThat(config.getVersionId()).isEqualTo("specific-version-to-delete");
    }
}
