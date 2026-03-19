package com.bonitasoft.connectors.s3;

import lombok.extern.slf4j.Slf4j;
import org.bonitasoft.engine.connector.AbstractConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;

import java.util.Map;

@Slf4j
public abstract class AbstractS3Connector extends AbstractConnector {

    protected static final String OUTPUT_SUCCESS = "success";
    protected static final String OUTPUT_ERROR_MESSAGE = "errorMessage";

    protected S3Configuration configuration;
    protected S3ConnectorClient client;

    @Override
    public void validateInputParameters() throws ConnectorValidationException {
        try {
            this.configuration = buildConfiguration();
            validateConfiguration(this.configuration);
        } catch (IllegalArgumentException e) {
            throw new ConnectorValidationException(this, e.getMessage());
        }
    }

    @Override
    public void connect() throws ConnectorException {
        try {
            this.client = new S3ConnectorClient(this.configuration);
            log.info("S3 connector connected successfully");
        } catch (S3Exception e) {
            throw new ConnectorException("Failed to connect: " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() throws ConnectorException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
    }

    @Override
    protected void executeBusinessLogic() throws ConnectorException {
        try {
            doExecute();
            setOutputParameter(OUTPUT_SUCCESS, true);
        } catch (S3Exception e) {
            log.error("S3 connector execution failed: {}", e.getMessage(), e);
            setOutputParameter(OUTPUT_SUCCESS, false);
            setOutputParameter(OUTPUT_ERROR_MESSAGE, truncate(e.getMessage()));
        } catch (Exception e) {
            log.error("Unexpected error in S3 connector: {}", e.getMessage(), e);
            setOutputParameter(OUTPUT_SUCCESS, false);
            setOutputParameter(OUTPUT_ERROR_MESSAGE, truncate("Unexpected error: " + e.getMessage()));
        }
    }

    protected abstract void doExecute() throws S3Exception;

    protected abstract S3Configuration buildConfiguration();

    protected void validateConfiguration(S3Configuration config) {
        if (config.getRegion() == null || config.getRegion().isBlank()) {
            throw new IllegalArgumentException("region is mandatory");
        }
        if (!Boolean.TRUE.equals(config.getUseDefaultCredentialChain())) {
            if (config.getAccessKeyId() == null || config.getAccessKeyId().isBlank()) {
                throw new IllegalArgumentException("accessKeyId is mandatory when not using default credential chain");
            }
            if (config.getSecretAccessKey() == null || config.getSecretAccessKey().isBlank()) {
                throw new IllegalArgumentException("secretAccessKey is mandatory when not using default credential chain");
            }
        }
    }

    protected String readStringInput(String name) {
        Object value = getInputParameter(name);
        return value != null ? value.toString() : null;
    }

    protected String readStringInput(String name, String defaultValue) {
        String value = readStringInput(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    protected Boolean readBooleanInput(String name, boolean defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? (Boolean) value : defaultValue;
    }

    protected Integer readIntegerInput(String name, int defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? ((Number) value).intValue() : defaultValue;
    }

    protected Long readLongInput(String name) {
        Object value = getInputParameter(name);
        return value != null ? ((Number) value).longValue() : null;
    }

    protected Long readLongInput(String name, long defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? ((Number) value).longValue() : defaultValue;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> readMapInput(String name) {
        Object value = getInputParameter(name);
        return value != null ? (Map<String, String>) value : null;
    }

    protected S3Configuration.S3ConfigurationBuilder connectionConfig() {
        return S3Configuration.builder()
                .accessKeyId(readStringInput("accessKeyId"))
                .secretAccessKey(readStringInput("secretAccessKey"))
                .sessionToken(readStringInput("sessionToken"))
                .region(readStringInput("region"))
                .useDefaultCredentialChain(readBooleanInput("useDefaultCredentialChain", false))
                .connectTimeout(readIntegerInput("connectTimeout", 30000))
                .readTimeout(readIntegerInput("readTimeout", 60000));
    }

    private String truncate(String message) {
        if (message == null) return "";
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    Map<String, Object> getOutputs() {
        return getOutputParameters();
    }
}
