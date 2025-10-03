package com.example.batchdelete.io;

import com.example.batchdelete.model.BlobDeleteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Reads {@link BlobDeleteRequest} entries from a CSV file.
 */
public class CsvBlobDeleteRequestReader {

    private static final Logger LOGGER = LogManager.getLogger(CsvBlobDeleteRequestReader.class);

    @FunctionalInterface
    private interface ReaderFactory {
        BufferedReader create() throws IOException;
    }

    private final ReaderFactory readerFactory;
    private final String sourceDescription;
    private final String separator;
    private final boolean hasHeader;

    public CsvBlobDeleteRequestReader(Path csvPath, String separator, boolean hasHeader) {
        this(createFileReaderFactory(csvPath), csvPath.toString(), separator, hasHeader);
    }

    private CsvBlobDeleteRequestReader(ReaderFactory readerFactory, String sourceDescription, String separator,
            boolean hasHeader) {
        this.readerFactory = readerFactory;
        this.sourceDescription = sourceDescription;
        this.separator = separator;
        this.hasHeader = hasHeader;
    }

    public static CsvBlobDeleteRequestReader forFile(Path csvPath, String separator, boolean hasHeader) {
        return new CsvBlobDeleteRequestReader(csvPath, separator, hasHeader);
    }

    public static CsvBlobDeleteRequestReader forContent(String csvContent, String separator, boolean hasHeader) {
        Objects.requireNonNull(csvContent, "csvContent");
        return new CsvBlobDeleteRequestReader(() -> new BufferedReader(new StringReader(csvContent)),
                "inline CSV content", separator, hasHeader);
    }

    public String getSourceDescription() {
        return sourceDescription;
    }

    public interface Consumer {
        void accept(List<BlobDeleteRequest> batch);
    }

    public void readBatches(int batchSize, Consumer consumer) throws IOException {
        Pattern splitPattern = Pattern.compile(Pattern.quote(separator));

        try (BufferedReader reader = readerFactory.create()) {
            String line;
            long lineNumber = 0;
            if (hasHeader) {
                reader.readLine();
                lineNumber++;
            }

            List<BlobDeleteRequest> currentBatch = new ArrayList<>(batchSize);

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                BlobDeleteRequest request = parseLine(line, lineNumber, splitPattern);
                if (request == null) {
                    continue;
                }

                currentBatch.add(request);

                if (currentBatch.size() == batchSize) {
                    consumer.accept(List.copyOf(currentBatch));
                    currentBatch.clear();
                }
            }

            if (!currentBatch.isEmpty()) {
                consumer.accept(List.copyOf(currentBatch));
            }
        }
    }

    public List<BlobDeleteRequest> readAllRequests() throws IOException {
        Pattern splitPattern = Pattern.compile(Pattern.quote(separator));
        List<BlobDeleteRequest> requests = new ArrayList<>();

        try (BufferedReader reader = readerFactory.create()) {
            String line;
            long lineNumber = 0;
            if (hasHeader) {
                reader.readLine();
                lineNumber++;
            }

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                BlobDeleteRequest request = parseLine(line, lineNumber, splitPattern);
                if (request != null) {
                    requests.add(request);
                }
            }
        }

        return requests;
    }

    private static String stripQuotes(String value) {
        String cleaned = value.replace("\"", "").trim();
        return cleaned;
    }

    private BlobDeleteRequest parseLine(String line, long lineNumber, Pattern splitPattern) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            LOGGER.warn("Skipping empty line {} in {}", lineNumber, sourceDescription);
            return null;
        }

        String[] tokens = splitPattern.split(trimmed, -1);
        if (tokens.length < 3) {
            LOGGER.error("Invalid CSV line {} in {}: expected at least 3 tokens but found {}. Line content: {}",
                    lineNumber, sourceDescription, tokens.length, line);
            return null;
        }

        String storageAccountName = stripQuotes(tokens[0]);
        String containerName = stripQuotes(tokens[1]);
        String blobName = stripQuotes(tokens[2]);
        if (storageAccountName.isEmpty() || containerName.isEmpty() || blobName.isEmpty()) {
            LOGGER.error("Invalid CSV line {} in {}: storage account, container or blob name missing. Line content: {}",
                    lineNumber, sourceDescription, line);
            return null;
        }

        return new BlobDeleteRequest(storageAccountName, containerName, blobName, lineNumber, line);
    }

    private static ReaderFactory createFileReaderFactory(Path csvPath) {
        return () -> Files.newBufferedReader(csvPath);
    }
}
