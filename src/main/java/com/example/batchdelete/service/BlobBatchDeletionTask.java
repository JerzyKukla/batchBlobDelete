package com.example.batchdelete.service;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchOperationResponse;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.example.batchdelete.model.BlobDeleteRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
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

        try {
            for (BlobDeleteRequest request : requests) {
                blobBatch.deleteBlob(request.getContainerName(), request.getBlobName(),
                        DeleteSnapshotsOptionType.INCLUDE, null);
            }

            Response<BlobBatchOperationResponse> response = blobBatchClient.submitBatchWithResponse(blobBatch, false,
                    DEFAULT_TIMEOUT, Context.NONE);
            BlobBatchOperationResponse operationResponse = response.getValue();
            List<Response<Void>> subResponses = operationResponse != null ? operationResponse.getSubResponses()
                    : Collections.emptyList();

            for (int i = 0; i < subResponses.size() && i < requests.size(); i++) {
                Response<Void> subResponse = subResponses.get(i);
                BlobDeleteRequest request = requests.get(i);
                int statusCode = subResponse.getStatusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    successCount++;
                    LOGGER.info("Successfully deleted blob {} from container {} (line {})", request.getBlobName(),
                            request.getContainerName(), request.getLineNumber());
                } else {
                    failureCount++;
                    String errorCode = subResponse.getHeaders().getValue("x-ms-error-code");
                    LOGGER.error("Failed to delete blob {} from container {} (line {}), status code {}, error code {}",
                            request.getBlobName(), request.getContainerName(), request.getLineNumber(), statusCode,
                            errorCode);
                }
            }

            if (subResponses.size() != requests.size()) {
                LOGGER.warn("Blob batch response count ({}) does not match request count ({})", subResponses.size(),
                        requests.size());
            }
        } catch (BlobBatchStorageException ex) {
            failureCount += requests.size();
            LOGGER.error("Azure Storage batch error when deleting blobs", ex);
        } catch (RuntimeException ex) {
            failureCount += requests.size();
            LOGGER.error("Unexpected error when deleting blob batch", ex);
        }

        return new BatchDeletionResult(successCount, failureCount);
    }
}
