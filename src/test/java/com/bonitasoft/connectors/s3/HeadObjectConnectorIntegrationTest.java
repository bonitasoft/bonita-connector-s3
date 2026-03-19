package com.bonitasoft.connectors.s3;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;

@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
class HeadObjectConnectorIntegrationTest {

    @Test
    void shouldExecuteAgainstRealApi() throws Exception {
        var connector = new HeadObjectConnector();
        var inputs = new HashMap<String, Object>();
        inputs.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
        inputs.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
        inputs.put("region", System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", System.getenv().getOrDefault("S3_TEST_OBJECT_KEY", "integration-test/test-download.txt"));
        connector.setInputParameters(inputs);
        connector.validateInputParameters();
        connector.connect();
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("contentType")).isNotNull();
        assertThat(connector.getOutputs().get("contentLengthBytes")).isNotNull();
        connector.disconnect();
    }
}
