package com.example.batchdelete.service;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.example.batchdelete.config.AppConfig;
import com.example.batchdelete.io.CsvBlobDeleteRequestReader;
import com.example.batchdelete.model.BlobDeleteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinates reading the CSV file and deleting the blobs in batches.
 */
public class BlobBatchDeletionService {

    private static final Logger LOGGER = LogManager.getLogger(BlobBatchDeletionService.class);
    private static final Duration TERMINATION_TIMEOUT = Duration.ofMinutes(2);
    private static final String DEFAULT_BLOB_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net";

    private final AppConfig config;
    private final TokenCredential credential;

    public BlobBatchDeletionService(AppConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.credential = new DefaultAzureCredentialBuilder().build();
    }

    public BatchDeletionResult execute() throws IOException, InterruptedException {
        CsvBlobDeleteRequestReader reader = createReaderFromConfig();
        return execute(reader);
    }

    public BatchDeletionResult executeWithCsvContent(String csvContent) throws IOException, InterruptedException {
        Objects.requireNonNull(csvContent, "csvContent");
        CsvBlobDeleteRequestReader reader = CsvBlobDeleteRequestReader.forContent(csvContent, config.getCsvSeparator(),
                config.isCsvHasHeader());
        return execute(reader);
    }

    public BatchDeletionResult executeWithInputFile(Path csvPath) throws IOException, InterruptedException {
        Objects.requireNonNull(csvPath, "csvPath");
        CsvBlobDeleteRequestReader reader = CsvBlobDeleteRequestReader.forFile(csvPath, config.getCsvSeparator(),
                config.isCsvHasHeader());
        return execute(reader);
    }

    private CsvBlobDeleteRequestReader createReaderFromConfig() {
        return config.getInputCsvContent()
                .map(content -> CsvBlobDeleteRequestReader.forContent(content, config.getCsvSeparator(),
                        config.isCsvHasHeader()))
                .orElseGet(() -> CsvBlobDeleteRequestReader
                        .forFile(config.getInputFilePath().map(Path::of)
                                .orElseThrow(() -> new IllegalStateException("No input source configured")),
                                config.getCsvSeparator(), config.isCsvHasHeader()));
    }

    private BatchDeletionResult execute(CsvBlobDeleteRequestReader reader) throws IOException, InterruptedException {

        List<BlobDeleteRequest> requests = reader.readAllRequests();
        LOGGER.info("Loaded {} blob delete requests from {}", requests.size(), reader.getSourceDescription());

        if (requests.isEmpty()) {
            LOGGER.info("No blob delete requests to process");
            return new BatchDeletionResult(0, 0);
        }

        Map<String, List<BlobDeleteRequest>> requestsByAccount = groupRequestsByStorageAccount(requests);
        LOGGER.info("Discovered {} storage account(s) in the input", requestsByAccount.size());

        ExecutorService executorService = Executors.newFixedThreadPool(config.getThreadPoolSize());
        List<CompletableFuture<BatchDeletionResult>> futures = new ArrayList<>();
        BatchDeletionResult totalResult = new BatchDeletionResult(0, 0);

        try {
            for (Map.Entry<String, List<BlobDeleteRequest>> entry : requestsByAccount.entrySet()) {
                String storageAccountName = entry.getKey();
                List<BlobDeleteRequest> accountRequests = entry.getValue();

                LOGGER.info("Scheduling {} request(s) for storage account {}", accountRequests.size(),
                        storageAccountName);

                BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                        .endpoint(resolveEndpoint(storageAccountName))
                        .credential(credential)
                        .buildClient();

                BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(blobServiceClient).buildClient();
                AtomicInteger nextIndex = new AtomicInteger(0);
                int workerCount = Math.min(config.getThreadPoolSize(), Math.max(1,
                        (int) Math.ceil((double) accountRequests.size() / config.getBatchSize())));

                for (int i = 0; i < workerCount; i++) {
                    CompletableFuture<BatchDeletionResult> future = CompletableFuture
                            .supplyAsync(
                                    () -> processQueue(storageAccountName, blobBatchClient, blobServiceClient,
                                            accountRequests, nextIndex, config.getBatchSize()),
                                    executorService)
                            .exceptionally(throwable -> {
                                Throwable cause = throwable instanceof CompletionException ? throwable.getCause()
                                        : throwable;
                                LOGGER.error("Batch execution failed for storage account {}", storageAccountName, cause);
                                return new BatchDeletionResult(0, 0);
                            });
                    futures.add(future);
                }
            }

            CompletableFuture<Void> allDone = CompletableFuture
                    .allOf(futures.toArray(new CompletableFuture[0]));
            try {
                allDone.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                LOGGER.error("Error awaiting batch completion", cause != null ? cause : e);
            }

            for (CompletableFuture<BatchDeletionResult> future : futures) {
                totalResult = totalResult.add(future.join());
            }

            LOGGER.info("Batch processing completed. Success: {}, Failure: {}", totalResult.successCount(),
                    totalResult.failureCount());
        } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(TERMINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Executor did not terminate within {} seconds", TERMINATION_TIMEOUT.toSeconds());
                executorService.shutdownNow();
            }
        }
        return totalResult;
    }

    private BatchDeletionResult processQueue(String storageAccountName, BlobBatchClient blobBatchClient,
            BlobServiceClient blobServiceClient, List<BlobDeleteRequest> requests, AtomicInteger nextIndex,
            int batchSize) {
        BatchDeletionResult totalResult = new BatchDeletionResult(0, 0);

        while (true) {
            int startIndex = nextIndex.getAndAdd(batchSize);
            if (startIndex >= requests.size()) {
                break;
            }

            int endIndex = Math.min(startIndex + batchSize, requests.size());
            List<BlobDeleteRequest> batch = new ArrayList<>(requests.subList(startIndex, endIndex));

            LOGGER.debug("Processing batch of {} blob(s) for storage account {}", batch.size(), storageAccountName);

            BlobBatchDeletionTask task = new BlobBatchDeletionTask(blobBatchClient, blobServiceClient, batch,
                    config.isSnapshotEnabled());
            BatchDeletionResult result = task.call();
            totalResult = totalResult.add(result);
        }

        return totalResult;
    }

    private static Map<String, List<BlobDeleteRequest>> groupRequestsByStorageAccount(List<BlobDeleteRequest> requests) {
        Map<String, List<BlobDeleteRequest>> grouped = new LinkedHashMap<>();
        for (BlobDeleteRequest request : requests) {
            grouped.computeIfAbsent(request.getStorageAccountName(), key -> new ArrayList<>()).add(request);
        }
        return grouped;
    }

    private static String resolveEndpoint(String storageAccountName) {
        String trimmed = storageAccountName.trim();
        if (trimmed.contains("://")) {
            return trimmed;
        }
        if (trimmed.contains(".")) {
            return "https://" + trimmed;
        }
        return String.format(DEFAULT_BLOB_ENDPOINT_TEMPLATE, trimmed);
    }
}
