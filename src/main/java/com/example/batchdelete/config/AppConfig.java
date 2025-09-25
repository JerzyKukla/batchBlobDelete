package com.example.batchdelete.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Holds configuration parameters for the Batch Blob Delete application.
 */
public final class AppConfig {

    private static final String DEFAULT_SEPARATOR = ",";
    private static final int MAX_BATCH_SIZE = 256;

    private final String inputFilePath;
    private final String storageEndpoint;
    private final int batchSize;
    private final int threadPoolSize;
    private final String csvSeparator;
    private final boolean csvHasHeader;
    private final boolean snapshotEnabled;

    private AppConfig(String inputFilePath, String storageEndpoint, int batchSize, int threadPoolSize,
            String csvSeparator, boolean csvHasHeader, boolean snapshotEnabled) {
        if (batchSize <= 0 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "batchSize must be between 1 and " + MAX_BATCH_SIZE + ", but was " + batchSize);
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize must be greater than zero");
        }
        this.inputFilePath = Objects.requireNonNull(inputFilePath, "inputFilePath");
        this.storageEndpoint = Objects.requireNonNull(storageEndpoint, "storageEndpoint");
        this.batchSize = batchSize;
        this.threadPoolSize = threadPoolSize;
        this.csvSeparator = csvSeparator == null || csvSeparator.isEmpty() ? DEFAULT_SEPARATOR : csvSeparator;
        this.csvHasHeader = csvHasHeader;
        this.snapshotEnabled = snapshotEnabled;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getStorageEndpoint() {
        return storageEndpoint;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public String getCsvSeparator() {
        return csvSeparator;
    }

    public boolean isCsvHasHeader() {
        return csvHasHeader;
    }

    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    public static AppConfig load(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        }

        String inputFilePath = requireProperty(properties, "inputFilePath");
        String storageEndpoint = requireProperty(properties, "storageEndpoint");
        int batchSize = parseIntProperty(properties, "batchSize", 255);
        int threadPoolSize = parseIntProperty(properties, "threadPoolSize", Runtime.getRuntime().availableProcessors());
        String csvSeparator = properties.getProperty("csvSeparator", DEFAULT_SEPARATOR);
        boolean csvHasHeader = parseBooleanProperty(properties, "csvHasHeader", true);
        boolean snapshotEnabled = parseBooleanProperty(properties, "snapshotEnable", false);

        return new AppConfig(inputFilePath, storageEndpoint, batchSize, threadPoolSize, csvSeparator, csvHasHeader,
                snapshotEnabled);
    }

    private static String requireProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property '" + propertyName + "'.");
        }
        return value;
    }

    private static int parseIntProperty(Properties properties, String propertyName, int defaultValue) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for property '" + propertyName + "': " + value,
                    e);
        }
    }

    private static boolean parseBooleanProperty(Properties properties, String propertyName, boolean defaultValue) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
