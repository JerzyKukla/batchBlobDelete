package com.example.batchdelete.service;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.example.batchdelete.model.BlobDeleteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Callable task that deletes a batch of blobs using {@link BlobBatchClient}.
 */
public class BlobBatchDeletionTask implements Callable<BatchDeletionResult> {

    private static final Logger LOGGER = LogManager.getLogger(BlobBatchDeletionTask.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(2);

    private final BlobBatchClient blobBatchClient;
    private final List<BlobDeleteRequest> requests;

    public BlobBatchDeletionTask(BlobBatchClient blobBatchClient, List<BlobDeleteRequest> requests) {
        this.blobBatchClient = blobBatchClient;
        this.requests = requests;
    }

    @Override
    public BatchDeletionResult call() {
        int successCount = 0;
        int failureCount = 0;
        BlobBatch blobBatch = blobBatchClient.getBlobBatch();
        List<Response<Void>> blobDeletionResponses = new ArrayList<>(requests.size());

        try {
            for (BlobDeleteRequest request : requests) {
                Response<Void> responseHandle = blobBatch.deleteBlob(request.getContainerName(), request.getBlobName(),
                        DeleteSnapshotsOptionType.INCLUDE, null);
                blobDeletionResponses.add(responseHandle);
            }

            Response<Void> batchResponse = blobBatchClient.submitBatchWithResponse(blobBatch, false,
                    DEFAULT_TIMEOUT, Context.NONE);
            if (batchResponse != null) {
                LOGGER.debug("Blob batch submission completed with status code {}", batchResponse.getStatusCode());
            }

            for (int i = 0; i < blobDeletionResponses.size() && i < requests.size(); i++) {
                Response<Void> subResponse = blobDeletionResponses.get(i);
                BlobDeleteRequest request = requests.get(i);
                int statusCode = subResponse.getStatusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    successCount++;
                    LOGGER.info("Successfully deleted blob {} from container {} (line {})", request.getBlobName(),
                            request.getContainerName(), request.getLineNumber());
                } else {
                    failureCount++;
                    String errorCode = subResponse.getHeaders().getValue("x-ms-error-code");
                    LOGGER.error(
                            "Failed to delete blob {} from container {} (line {}), status code {}, error code {}. Line context: {}",
                            request.getBlobName(), request.getContainerName(), request.getLineNumber(), statusCode,
                            errorCode, formatLineContext(request));
                }
            }

            if (blobDeletionResponses.size() != requests.size()) {
                LOGGER.warn("Blob batch response count ({}) does not match request count ({})", blobDeletionResponses.size(),
                        requests.size());
            }
        } catch (BlobBatchStorageException ex) {
            failureCount += requests.size();
            LOGGER.error("Azure Storage batch error when deleting blobs. Lines: {}", formatLineContexts(requests), ex);
        } catch (RuntimeException ex) {
            failureCount += requests.size();
            LOGGER.error("Unexpected error when deleting blob batch. Lines: {}", formatLineContexts(requests), ex);
        }

        return new BatchDeletionResult(successCount, failureCount);
    }

    private static String formatLineContext(BlobDeleteRequest request) {
        return "line " + request.getLineNumber() + ": " + request.getRawLine();
    }

    private static String formatLineContexts(List<BlobDeleteRequest> requests) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < requests.size(); i++) {
            if (i > 0) {
                builder.append("; ");
            }
            builder.append(formatLineContext(requests.get(i)));
        }
        return builder.toString();
    }
}
