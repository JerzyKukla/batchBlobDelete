package com.example.batchdelete.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.example.batchdelete.config.AppConfig;
import com.example.batchdelete.io.CsvBlobDeleteRequestReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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

        ExecutorService executorService = Executors.newFixedThreadPool(config.getThreadPoolSize());
        List<CompletableFuture<BatchDeletionResult>> futures = new ArrayList<>();

        try {
            reader.readBatches(config.getBatchSize(), batch -> {
                BlobBatchDeletionTask task = new BlobBatchDeletionTask(blobBatchClient, batch);
                CompletableFuture<BatchDeletionResult> future = CompletableFuture.supplyAsync(task::call,
                        executorService)
                        .exceptionally(ex -> {
                            LOGGER.error("Batch execution failed", ex);
                            return new BatchDeletionResult(0, 0);
                        });
                synchronized (futures) {
                    futures.add(future);
                }
            });

            CompletableFuture<?>[] futuresArray = futures.toArray(new CompletableFuture[0]);
            try {
                CompletableFuture.allOf(futuresArray).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                LOGGER.error("Batch execution failed", e.getCause());
            }

            BatchDeletionResult totalResult = futures.stream()
                    .map(CompletableFuture::join)
                    .reduce(new BatchDeletionResult(0, 0), BatchDeletionResult::add);

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
}
