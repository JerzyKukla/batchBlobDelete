package com.example.batchdelete.io;

import com.example.batchdelete.model.BlobDeleteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads {@link BlobDeleteRequest} entries from a CSV file.
 */
public class CsvBlobDeleteRequestReader {

    private static final Logger LOGGER = LogManager.getLogger(CsvBlobDeleteRequestReader.class);

    private final Path csvPath;
    private final String separator;
    private final boolean hasHeader;

    public CsvBlobDeleteRequestReader(Path csvPath, String separator, boolean hasHeader) {
        this.csvPath = csvPath;
        this.separator = separator;
        this.hasHeader = hasHeader;
    }

    public interface Consumer {
        void accept(List<BlobDeleteRequest> batch);
    }

    public void readBatches(int batchSize, Consumer consumer) throws IOException {
        Pattern splitPattern = Pattern.compile(Pattern.quote(separator));

        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
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

        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
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
            LOGGER.warn("Skipping empty line {} in {}", lineNumber, csvPath);
            return null;
        }

        String[] tokens = splitPattern.split(trimmed, -1);
        if (tokens.length < 2) {
            LOGGER.error("Invalid CSV line {} in {}: expected at least 2 tokens but found {}. Line content: {}",
                    lineNumber, csvPath, tokens.length, line);
            return null;
        }

        String containerName = stripQuotes(tokens[0]);
        String blobName = stripQuotes(tokens[1]);
        if (containerName.isEmpty() || blobName.isEmpty()) {
            LOGGER.error("Invalid CSV line {} in {}: container or blob name missing. Line content: {}",
                    lineNumber, csvPath, line);
            return null;
        }

        return new BlobDeleteRequest(containerName, blobName, lineNumber, line);
    }
}
