package com.example.batchdelete.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

/**
 * Holds configuration parameters for the Batch Blob Delete application.
 */
public final class AppConfig {

    private static final String DEFAULT_SEPARATOR = ",";
    private static final int MAX_BATCH_SIZE = 256;

    private final String inputFilePath;
    private final String inputCsvContent;
    private final int batchSize;
    private final int threadPoolSize;
    private final String csvSeparator;
    private final boolean csvHasHeader;
    private final boolean snapshotEnabled;

    private AppConfig(String inputFilePath, String inputCsvContent, int batchSize,
            int threadPoolSize, String csvSeparator, boolean csvHasHeader, boolean snapshotEnabled) {
        if (batchSize <= 0 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "batchSize must be between 1 and " + MAX_BATCH_SIZE + ", but was " + batchSize);
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize must be greater than zero");
        }
        if ((inputFilePath == null || inputFilePath.isBlank()) && (inputCsvContent == null || inputCsvContent.isBlank())) {
            throw new IllegalArgumentException("Either inputFilePath or inputCsvContent must be provided.");
        }
        if ((inputFilePath != null && !inputFilePath.isBlank()) && (inputCsvContent != null && !inputCsvContent.isBlank())) {
            throw new IllegalArgumentException("Only one of inputFilePath or inputCsvContent can be provided.");
        }
        this.inputFilePath = inputFilePath == null || inputFilePath.isBlank() ? null : inputFilePath;
        this.inputCsvContent = inputCsvContent == null || inputCsvContent.isBlank() ? null : inputCsvContent;
        this.batchSize = batchSize;
        this.threadPoolSize = threadPoolSize;
        this.csvSeparator = csvSeparator == null || csvSeparator.isEmpty() ? DEFAULT_SEPARATOR : csvSeparator;
        this.csvHasHeader = csvHasHeader;
        this.snapshotEnabled = snapshotEnabled;
    }

    public Optional<String> getInputFilePath() {
        return Optional.ofNullable(inputFilePath);
    }

    public Optional<String> getInputCsvContent() {
        return Optional.ofNullable(inputCsvContent);
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
        return load(path, null, null, null, null);
    }

    public static AppConfig load(Path path, String overrideInputFilePath, String overrideInputCsvContent,
            Integer overrideBatchSize, Integer overrideThreadPoolSize) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        }

        validateMutuallyExclusive(properties);

        String inputFilePath = firstNonBlank(overrideInputFilePath, properties.getProperty("inputFilePath"));
        String inputCsvContent = firstNonBlank(overrideInputCsvContent, properties.getProperty("inputCsvContent"));
        if (!isBlank(inputCsvContent)) {
            inputFilePath = null;
        }
        if (isBlank(inputFilePath) && isBlank(inputCsvContent)) {
            throw new IllegalArgumentException(
                    "Either inputFilePath or inputCsvContent must be provided (config or CLI override).");
        }

        int batchSize = overrideBatchSize != null ? overrideBatchSize : parseIntProperty(properties, "batchSize", 255);
        int threadPoolSize = overrideThreadPoolSize != null ? overrideThreadPoolSize
                : parseIntProperty(properties, "threadPoolSize", Runtime.getRuntime().availableProcessors());
        String csvSeparator = properties.getProperty("csvSeparator", DEFAULT_SEPARATOR);
        boolean csvHasHeader = parseBooleanProperty(properties, "csvHasHeader", true);
        boolean snapshotEnabled = parseBooleanProperty(properties, "snapshotEnable", false);

        return new AppConfig(inputFilePath, inputCsvContent, batchSize, threadPoolSize, csvSeparator,
                csvHasHeader, snapshotEnabled);
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

    private static void validateMutuallyExclusive(Properties properties) {
        String filePath = properties.getProperty("inputFilePath");
        String csvContent = properties.getProperty("inputCsvContent");
        if (!isBlank(filePath) && !isBlank(csvContent)) {
            throw new IllegalArgumentException(
                    "Properties inputFilePath and inputCsvContent are mutually exclusive. Please configure only one.");
        }
    }

    private static String firstNonBlank(String first, String second) {
        if (!isBlank(first)) {
            return first;
        }
        return isBlank(second) ? null : second;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
