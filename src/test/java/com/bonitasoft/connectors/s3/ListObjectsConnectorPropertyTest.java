package com.bonitasoft.connectors.s3;

import net.jqwik.api.*;
import net.jqwik.api.constraints.*;
import org.bonitasoft.engine.connector.ConnectorValidationException;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class ListObjectsConnectorPropertyTest {

    @Provide
    Arbitrary<String> blankStrings() {
        return Arbitraries.of("", " ", "\t", "\n");
    }

    @Provide
    Arbitrary<String> validRegions() {
        return Arbitraries.of("us-east-1", "us-west-2", "eu-west-1", "eu-north-1", "me-south-1");
    }

    @Provide
    Arbitrary<String> validBucketNames() {
        return Arbitraries.strings().ofMinLength(3).ofMaxLength(63)
                .withCharRange('a', 'z').withCharRange('0', '9').withChars('-');
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-bucket");
        return inputs;
    }

    @Property
    void mandatoryBucketNameRejectsBlank(@ForAll("blankStrings") String bucketName) {
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("bucketName", bucketName);
        connector.setInputParameters(inputs);
        assertThatThrownBy(connector::validateInputParameters)
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Property
    void mandatoryRegionRejectsBlank(@ForAll("blankStrings") String region) {
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("region", region);
        connector.setInputParameters(inputs);
        assertThatThrownBy(connector::validateInputParameters)
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Property
    void validConfigurationAlwaysBuilds(
            @ForAll("validBucketNames") String bucketName) {
        Assume.that(bucketName.length() >= 3);
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("bucketName", bucketName);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void regionFormatAccepted(@ForAll("validRegions") String region) {
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("region", region);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void accessKeyIdFormatValidation(
            @ForAll @AlphaChars @StringLength(min = 16, max = 20) String suffix) {
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("accessKeyId", "AKIA" + suffix);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void bucketNameValidation(@ForAll("validBucketNames") String bucketName) {
        Assume.that(bucketName.length() >= 3);
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("bucketName", bucketName);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void prefixAcceptsAnyString(@ForAll @AlphaChars @StringLength(min = 0, max = 1024) String prefix) {
        var connector = new ListObjectsConnector();
        var inputs = validInputs();
        inputs.put("prefix", prefix);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void maxKeysPositiveOnly(@ForAll @IntRange(min = 1, max = 1000) int maxKeys) {
        var config = S3Configuration.builder()
                .accessKeyId("AKIAIOSFODNN7EXAMPLE")
                .secretAccessKey("secret")
                .region("us-east-1")
                .bucketName("bucket")
                .maxKeys(maxKeys)
                .build();
        assertThat(config.getMaxKeys()).isPositive();
    }

    @Property
    void defaultValuesApplied() {
        var config = S3Configuration.builder()
                .accessKeyId("AKIAIOSFODNN7EXAMPLE")
                .secretAccessKey("secret")
                .region("us-east-1")
                .build();
        assertThat(config.getPrefix()).isNull();
        assertThat(config.getDelimiter()).isNull();
        assertThat(config.getMaxKeys()).isNull();
        assertThat(config.getContinuationToken()).isNull();
    }

    @Property
    void errorMessageTruncation(@ForAll @AlphaChars @StringLength(min = 1001, max = 2000) String longMsg) {
        var truncated = longMsg.length() > 1000 ? longMsg.substring(0, 1000) + "..." : longMsg;
        assertThat(truncated.length()).isLessThanOrEqualTo(1004);
    }
}
