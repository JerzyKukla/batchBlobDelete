package com.example.batchdelete.service;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatch;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.models.BlobStorageException;
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
    private final BlobServiceClient blobServiceClient;
    private final List<BlobDeleteRequest> requests;
    private final boolean snapshotEnabled;

    public BlobBatchDeletionTask(BlobBatchClient blobBatchClient, BlobServiceClient blobServiceClient,
                                 List<BlobDeleteRequest> requests, boolean snapshotEnabled) {
        this.blobBatchClient = blobBatchClient;
        this.blobServiceClient = blobServiceClient;
        this.requests = requests;
        this.snapshotEnabled = snapshotEnabled;
    }

    @Override
    public BatchDeletionResult call() {
        int successCount = 0;
        int failureCount = 0;
        BlobBatch blobBatch = blobBatchClient.getBlobBatch();
        List<Response<Void>> blobDeletionResponses = new ArrayList<>(requests.size());
        List<BlobDeleteRequest> submittedRequests = new ArrayList<>(requests.size());

        try {
            for (BlobDeleteRequest request : requests) {
                if (snapshotEnabled) {
                    createSnapshot(request);
                }

                try {
                    Response<Void> responseHandle = blobBatch.deleteBlob(request.getContainerName(),
                            request.getBlobName(), DeleteSnapshotsOptionType.INCLUDE, null);
                    blobDeletionResponses.add(responseHandle);
                    submittedRequests.add(request);
                } catch (RuntimeException ex) {
                    failureCount++;
                    LOGGER.error("Failed to queue deletion for blob {} from container {} (line {}). Line context: {}",
                            request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                            formatLineContext(request), ex);
                }
            }

            if (!submittedRequests.isEmpty()) {
                Response<Void> batchResponse = null;
                try {
                    batchResponse = blobBatchClient.submitBatchWithResponse(blobBatch, false,
                            DEFAULT_TIMEOUT, Context.NONE);
                } catch (BlobBatchStorageException ex) {
                    LOGGER.error("Azure Storage batch error when deleting blobs. Lines: {}", formatLineContexts(submittedRequests),
                            ex);
                } catch (RuntimeException ex) {
                    failureCount += submittedRequests.size();
                    LOGGER.error("Unexpected error when submitting blob batch. Lines: {}",
                            formatLineContexts(submittedRequests), ex);
                    for (BlobDeleteRequest failedRequest : submittedRequests) {
                        LOGGER.error(
                                "Failed to delete blob {} from container {} (line {}) due to batch submission error. Line context: {}",
                                failedRequest.getBlobName(), failedRequest.getContainerName(),
                                failedRequest.getLineNumber(), formatLineContext(failedRequest));
                    }
                    return new BatchDeletionResult(successCount, failureCount);
                }

                if (batchResponse != null) {
                    LOGGER.debug("Blob batch submission completed with status code {}", batchResponse.getStatusCode());
                }
            }

            for (int i = 0; i < blobDeletionResponses.size() && i < submittedRequests.size(); i++) {
                Response<Void> subResponse = blobDeletionResponses.get(i);
                BlobDeleteRequest request = submittedRequests.get(i);
                try {
                    int statusCode = subResponse.getStatusCode();
                    if (statusCode >= 200 && statusCode < 300) {
                        successCount++;
                        LOGGER.info("Successfully deleted blob {} from container {} (line {})", request.getBlobName(),
                                request.getContainerName(), request.getLineNumber());
                    }
                } catch (BlobStorageException ex) {
                    failureCount++;
                    LOGGER.error(
                            "Failed to delete blob {} from container {} (line {}), status code {}, error code {}, service message {}. Line context: {}",
                            request.getBlobName(), request.getContainerName(), request.getLineNumber(), ex.getStatusCode(),
                            ex.getErrorCode(), ex.getServiceMessage(), formatLineContext(request));
                } catch (RuntimeException ex) {
                    failureCount++;
                    LOGGER.error("Unexpected error when processing deletion response for blob {} from container {} (line {}). Line context: {}",
                            request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                            formatLineContext(request), ex);
                }
            }

            if (blobDeletionResponses.size() != submittedRequests.size()) {
                LOGGER.warn("Blob batch response count ({}) does not match request count ({})", blobDeletionResponses.size(),
                        submittedRequests.size());
            }
        } catch (RuntimeException ex) {
            failureCount += submittedRequests.isEmpty() ? requests.size() : submittedRequests.size();
            LOGGER.error("Unexpected error when deleting blob batch. Lines: {}",
                    formatLineContexts(submittedRequests.isEmpty() ? requests : submittedRequests), ex);
            List<BlobDeleteRequest> failedRequests = submittedRequests.isEmpty() ? requests : submittedRequests;
            for (BlobDeleteRequest failedRequest : failedRequests) {
                LOGGER.error(
                        "Failed to delete blob {} from container {} (line {}) due to unexpected batch error. Line context: {}",
                        failedRequest.getBlobName(), failedRequest.getContainerName(), failedRequest.getLineNumber(),
                        formatLineContext(failedRequest));
            }
        }

        return new BatchDeletionResult(successCount, failureCount);
    }

    private void createSnapshot(BlobDeleteRequest request) {
        try {
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(request.getContainerName());
            if (!containerClient.exists()) {
                LOGGER.warn("Container {} not found when creating snapshot for blob {} (line {}). Line context: {}",
                        request.getContainerName(), request.getBlobName(), request.getLineNumber(),
                        formatLineContext(request));
                return;
            }

            BlobClient blobClient = containerClient.getBlobClient(request.getBlobName());
            if (!blobClient.exists()) {
                LOGGER.warn("Blob {} not found when creating snapshot in container {} (line {}). Line context: {}",
                        request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                        formatLineContext(request));
                return;
            }

            BlobClientBase snapshotInfo = blobClient.createSnapshot();
            LOGGER.info("Created snapshot {} for blob {} in container {} (line {})", snapshotInfo.getSnapshotId(),
                    request.getBlobName(), request.getContainerName(), request.getLineNumber());
        } catch (BlobStorageException ex) {
            if (ex.getStatusCode() == 404) {
                LOGGER.warn("Blob or container not found when creating snapshot for {} in container {} (line {}). Line context: {}",
                        request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                        formatLineContext(request));
            } else {
                LOGGER.error("Failed to create snapshot for blob {} in container {} (line {}). Line context: {}",
                        request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                        formatLineContext(request), ex);
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Unexpected error while creating snapshot for blob {} in container {} (line {}). Line context: {}",
                    request.getBlobName(), request.getContainerName(), request.getLineNumber(),
                    formatLineContext(request), ex);
        }
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
