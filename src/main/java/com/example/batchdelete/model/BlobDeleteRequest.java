package com.example.batchdelete.model;

import java.util.Objects;

/**
 * Represents a single blob to be deleted.
 */
public final class BlobDeleteRequest {

    private final String containerName;
    private final String blobName;
    private final long lineNumber;
    private final String rawLine;

    public BlobDeleteRequest(String containerName, String blobName, long lineNumber, String rawLine) {
        this.containerName = Objects.requireNonNull(containerName, "containerName");
        this.blobName = Objects.requireNonNull(blobName, "blobName");
        this.lineNumber = lineNumber;
        this.rawLine = rawLine;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getBlobName() {
        return blobName;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public String getRawLine() {
        return rawLine;
    }

    @Override
    public String toString() {
        return "BlobDeleteRequest{" +
                "containerName='" + containerName + '\'' +
                ", blobName='" + blobName + '\'' +
                ", lineNumber=" + lineNumber +
                '}';
    }
}
