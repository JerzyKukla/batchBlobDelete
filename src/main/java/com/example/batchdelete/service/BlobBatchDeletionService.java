package com.example.batchdelete.service;

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
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates reading the CSV file and deleting the blobs in batches.
 */
public class BlobBatchDeletionService {

    private static final Logger LOGGER = LogManager.getLogger(BlobBatchDeletionService.class);
    private static final Duration TERMINATION_TIMEOUT = Duration.ofMinutes(2);

    private final AppConfig config;

    public BlobBatchDeletionService(AppConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public void execute() throws IOException, InterruptedException {
        Path csvPath = Path.of(config.getInputFilePath());
        CsvBlobDeleteRequestReader reader = new CsvBlobDeleteRequestReader(csvPath, config.getCsvSeparator(),
                config.isCsvHasHeader());

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(config.getStorageEndpoint())
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(blobServiceClient).buildClient();

        List<BlobDeleteRequest> requests = reader.readAllRequests();
        Queue<BlobDeleteRequest> requestQueue = new ConcurrentLinkedQueue<>(requests);
        LOGGER.info("Loaded {} blob delete requests from {}", requestQueue.size(), csvPath);

        ExecutorService executorService = Executors.newFixedThreadPool(config.getThreadPoolSize());
        List<CompletableFuture<BatchDeletionResult>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < config.getThreadPoolSize(); i++) {
                CompletableFuture<BatchDeletionResult> future = CompletableFuture
                        .supplyAsync(() -> processQueue(blobBatchClient, requestQueue, config.getBatchSize()),
                                executorService)
                        .exceptionally(throwable -> {
                            Throwable cause = throwable instanceof CompletionException ? throwable.getCause()
                                    : throwable;
                            LOGGER.error("Batch execution failed", cause);
                            return new BatchDeletionResult(0, 0);
                        });
                futures.add(future);
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

            BatchDeletionResult totalResult = new BatchDeletionResult(0, 0);
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
    }

    private BatchDeletionResult processQueue(BlobBatchClient blobBatchClient, Queue<BlobDeleteRequest> requestQueue,
                                             int batchSize) {
        BatchDeletionResult totalResult = new BatchDeletionResult(0, 0);

        while (true) {
            List<BlobDeleteRequest> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                BlobDeleteRequest request = requestQueue.poll();
                if (request == null) {
                    break;
                }
                batch.add(request);
            }

            if (batch.isEmpty()) {
                break;
            }

            BlobBatchDeletionTask task = new BlobBatchDeletionTask(blobBatchClient, batch);
            BatchDeletionResult result = task.call();
            totalResult = totalResult.add(result);
        }

        return totalResult;
    }
}
