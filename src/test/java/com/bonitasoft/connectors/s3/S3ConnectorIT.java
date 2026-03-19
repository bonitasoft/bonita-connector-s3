package com.bonitasoft.connectors.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.bonitasoft.web.client.BonitaClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Process-based integration test: packages each connector into a Bonita process,
 * deploys to a Testcontainers Bonita instance, and verifies output process variables.
 */
@Slf4j
@Testcontainers
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
class S3ConnectorIT {

    private static final String BONITA_VERSION = "10.2.0";
    private static final String DEF_VERSION = "1.0.0";

    @Container
    static GenericContainer<?> bonitaContainer = new GenericContainer<>("bonita:" + BONITA_VERSION)
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/bonita").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3));

    private static BonitaClient bonitaClient;

    @BeforeAll
    static void setUpBonita() {
        String bonitaUrl = "http://" + bonitaContainer.getHost() + ":"
                + bonitaContainer.getMappedPort(8080) + "/bonita";
        bonitaClient = BonitaClient.builder(bonitaUrl).build();
        bonitaClient.login("install", "install");
        log.info("Connected to Bonita at {}", bonitaUrl);
    }

    @AfterAll
    static void tearDown() {
        if (bonitaClient != null) {
            bonitaClient.logout();
        }
    }

    private Map<String, String> commonInputs() {
        var inputs = new LinkedHashMap<String, String>();
        inputs.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
        inputs.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
        inputs.put("region", System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        inputs.put("useDefaultCredentialChain", "false");
        return inputs;
    }

    private void assertProcessStarted(String caseId) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var value = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
            assertThat(value).isNotNull();
        });
    }

    @Test
    void shouldUploadObject() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", "it-test/upload-" + System.currentTimeMillis() + ".txt");
        inputs.put("fileContentBase64", java.util.Base64.getEncoder().encodeToString("Hello IT".getBytes()));

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("objectUrl", ConnectorTestToolkit.Output.create("objectUrl", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-upload-object", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldDownloadObject() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", System.getenv().getOrDefault("S3_TEST_OBJECT_KEY", "it-test/test-download.txt"));

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("fileContentBase64", ConnectorTestToolkit.Output.create("fileContentBase64", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-download-object", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldDeleteObject() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", "it-test/delete-" + System.currentTimeMillis() + ".txt");

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("deletedObjectKey", ConnectorTestToolkit.Output.create("deletedObjectKey", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-delete-object", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldCopyObject() throws Exception {
        var inputs = commonInputs();
        String bucket = System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test");
        inputs.put("sourceBucketName", bucket);
        inputs.put("sourceObjectKey", System.getenv().getOrDefault("S3_TEST_OBJECT_KEY", "it-test/test-download.txt"));
        inputs.put("destinationBucketName", bucket);
        inputs.put("destinationObjectKey", "it-test/copy-" + System.currentTimeMillis() + ".txt");

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("destinationObjectKey", ConnectorTestToolkit.Output.create("destinationObjectKey", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-copy-object", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldHeadObject() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", System.getenv().getOrDefault("S3_TEST_OBJECT_KEY", "it-test/test-download.txt"));

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("contentType", ConnectorTestToolkit.Output.create("contentType", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-head-object", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldListObjects() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("prefix", "it-test/");

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("totalCount", ConnectorTestToolkit.Output.create("totalCount", Integer.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-list-objects", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldGeneratePresignedUrl() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", System.getenv().getOrDefault("S3_TEST_OBJECT_KEY", "it-test/test-download.txt"));
        inputs.put("httpMethod", "GET");

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("presignedUrl", ConnectorTestToolkit.Output.create("presignedUrl", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-generate-presigned-url", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }

    @Test
    void shouldMultipartUpload() throws Exception {
        var inputs = commonInputs();
        inputs.put("bucketName", System.getenv().getOrDefault("S3_TEST_BUCKET", "bonita-connector-test"));
        inputs.put("objectKey", "it-test/multipart-" + System.currentTimeMillis() + ".txt");
        inputs.put("fileContentBase64", java.util.Base64.getEncoder().encodeToString(
                "Multipart upload IT content".getBytes()));

        var outputs = new LinkedHashMap<String, ConnectorTestToolkit.Output>();
        outputs.put("success", ConnectorTestToolkit.Output.create("success", Boolean.class.getName()));
        outputs.put("objectUrl", ConnectorTestToolkit.Output.create("objectUrl", String.class.getName()));

        var bar = ConnectorTestToolkit.buildConnectorToTest("s3-multipart-upload", DEF_VERSION, inputs, outputs);
        var response = ConnectorTestToolkit.importAndLaunchProcess(bar, bonitaClient);
        String caseId = response.getCaseId();
        assertProcessStarted(caseId);

        var success = ConnectorTestToolkit.getProcessVariableValue(bonitaClient, caseId, "success");
        assertThat(success).isEqualTo("true");
    }
}
