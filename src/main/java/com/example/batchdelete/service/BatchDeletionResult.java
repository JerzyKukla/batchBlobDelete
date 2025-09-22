package com.example.batchdelete.service;

/**
 * Result for a single batch deletion execution.
 */
public record BatchDeletionResult(int successCount, int failureCount) {

    public BatchDeletionResult add(BatchDeletionResult other) {
        return new BatchDeletionResult(this.successCount + other.successCount,
                this.failureCount + other.failureCount);
    }
}
