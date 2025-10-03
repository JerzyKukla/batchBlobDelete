package com.example.batchdelete.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Result for a single batch deletion execution.
 */

public record BatchDeletionResult(int successCount, int failureCount, List<String> successMessages,
        List<String> failureMessages) {

    public BatchDeletionResult {
        successMessages = List.copyOf(successMessages);
        failureMessages = List.copyOf(failureMessages);
    }

    public BatchDeletionResult(int successCount, int failureCount) {
        this(successCount, failureCount, List.of(), List.of());
    }

    public BatchDeletionResult add(BatchDeletionResult other) {
        List<String> combinedSuccessMessages = new ArrayList<>(this.successMessages);
        combinedSuccessMessages.addAll(other.successMessages);

        List<String> combinedFailureMessages = new ArrayList<>(this.failureMessages);
        combinedFailureMessages.addAll(other.failureMessages);

        return new BatchDeletionResult(this.successCount + other.successCount,
                this.failureCount + other.failureCount,
                combinedSuccessMessages,
                combinedFailureMessages);
    }
}
