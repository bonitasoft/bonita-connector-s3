package com.bonitasoft.connectors.s3;

import net.jqwik.api.*;
import net.jqwik.api.constraints.*;
import org.bonitasoft.engine.connector.ConnectorValidationException;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class GeneratePresignedUrlConnectorPropertyTest {

    @Provide
    Arbitrary<String> blankStrings() {
        return Arbitraries.of("", " ", "\t", "\n");
    }

    @Provide
    Arbitrary<String> validRegions() {
        return Arbitraries.of("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "af-south-1");
    }

    @Provide
    Arbitrary<String> validBucketNames() {
        return Arbitraries.strings().ofMinLength(3).ofMaxLength(63)
                .withCharRange('a', 'z').withCharRange('0', '9').withChars('-');
    }

    @Provide
    Arbitrary<String> validHttpMethods() {
        return Arbitraries.of("GET", "PUT", "DELETE", "HEAD");
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
        inputs.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        inputs.put("region", "us-east-1");
        inputs.put("useDefaultCredentialChain", false);
        inputs.put("bucketName", "my-bucket");
        inputs.put("objectKey", "test.txt");
        inputs.put("httpMethod", "GET");
        return inputs;
    }

    @Property
    void mandatoryBucketNameRejectsBlank(@ForAll("blankStrings") String bucketName) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("bucketName", bucketName);
        connector.setInputParameters(inputs);
        assertThatThrownBy(connector::validateInputParameters)
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Property
    void mandatoryObjectKeyRejectsBlank(@ForAll("blankStrings") String objectKey) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("objectKey", objectKey);
        connector.setInputParameters(inputs);
        assertThatThrownBy(connector::validateInputParameters)
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Property
    void validConfigurationAlwaysBuilds(
            @ForAll @AlphaChars @StringLength(min = 1, max = 100) String objectKey,
            @ForAll("validHttpMethods") String method) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("objectKey", objectKey);
        inputs.put("httpMethod", method);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void regionFormatAccepted(@ForAll("validRegions") String region) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("region", region);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void accessKeyIdFormatValidation(
            @ForAll @AlphaChars @StringLength(min = 16, max = 20) String suffix) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("accessKeyId", "AKIA" + suffix);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void bucketNameValidation(@ForAll("validBucketNames") String bucketName) {
        Assume.that(bucketName.length() >= 3);
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("bucketName", bucketName);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void objectKeyMaxLength(@ForAll @AlphaChars @StringLength(min = 1, max = 1024) String key) {
        var connector = new GeneratePresignedUrlConnector();
        var inputs = validInputs();
        inputs.put("objectKey", key);
        connector.setInputParameters(inputs);
        assertThatCode(connector::validateInputParameters).doesNotThrowAnyException();
    }

    @Property
    void expirationSecondsPositiveOnly(@ForAll @IntRange(min = 1, max = 604800) int expiration) {
        var config = S3Configuration.builder()
                .accessKeyId("AKIAIOSFODNN7EXAMPLE")
                .secretAccessKey("secret")
                .region("us-east-1")
                .bucketName("bucket")
                .objectKey("key")
                .httpMethod("GET")
                .expirationSeconds(expiration)
                .build();
        assertThat(config.getExpirationSeconds()).isPositive();
    }

    @Property
    void defaultValuesApplied() {
        var config = S3Configuration.builder()
                .accessKeyId("AKIAIOSFODNN7EXAMPLE")
                .secretAccessKey("secret")
                .region("us-east-1")
                .build();
        assertThat(config.getExpirationSeconds()).isNull();
        assertThat(config.getHttpMethod()).isNull();
    }

    @Property
    void errorMessageTruncation(@ForAll @AlphaChars @StringLength(min = 1001, max = 2000) String longMsg) {
        var truncated = longMsg.length() > 1000 ? longMsg.substring(0, 1000) + "..." : longMsg;
        assertThat(truncated.length()).isLessThanOrEqualTo(1004);
    }
}
