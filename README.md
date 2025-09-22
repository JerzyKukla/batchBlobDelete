# Batch Blob Delete Application

This project provides a command line utility that deletes blobs from an Azure Storage account in batches using the [`BlobBatchClient`](https://learn.microsoft.com/java/api/com.azure.storage.blob.batch.blobbatchclient) API. The application reads the list of blobs to remove from a CSV file, groups the operations into batches (up to 255 blobs each by default) and executes them concurrently using a fixed size `ExecutorService`.

## Features

- Reads the CSV input file line by line to build deletion requests.
- Configurable batch size, CSV separator and thread pool size.
- Uses Azure `DefaultAzureCredential` so it can run inside environments with managed identity (e.g. AKS pods).
- Logs every successful and failed deletion using Log4j 2.
- Builds as an executable (fat) JAR using the Maven Shade Plugin.

## Project Structure

```
src/main/java/com/example/batchdelete
├── BatchBlobDeleteApplication.java
├── config/AppConfig.java
├── io/CsvBlobDeleteRequestReader.java
├── model/BlobDeleteRequest.java
└── service
    ├── BatchDeletionResult.java
    ├── BlobBatchDeletionService.java
    └── BlobBatchDeletionTask.java
```

## Configuration

Runtime configuration is provided through a properties file. An example file is available at `config/application.properties`:

```
# Path to the input CSV file containing container and blob names to delete.
inputFilePath=./data/input.csv

# Azure Blob Storage service endpoint, e.g. https://<account-name>.blob.core.windows.net
storageEndpoint=https://your-storage-account.blob.core.windows.net

# Maximum number of blobs per batch (Azure limit is 256; default is 255)
batchSize=255

# Number of concurrent worker threads
threadPoolSize=4

# CSV separator character (default is comma)
csvSeparator=,

# Indicates whether the CSV file includes a header row (default true)
csvHasHeader=true
```

At runtime the application looks for `config/application.properties` by default. You can pass a different configuration file path as the first command line argument.

## CSV Format

The CSV file must contain at least two columns per line:

1. Container name.
2. Blob name (relative path inside the container).

Additional columns are ignored. If the file includes a header row, make sure `csvHasHeader` is set to `true` (default).

Example:

```
container,blob
invoices,2024/04/01/invoice-001.pdf
invoices,2024/04/01/invoice-002.pdf
archive,backup/file.zip
```

## Building

Use Maven to build the shaded JAR:

```bash
mvn package
```

The output artifact will be generated at `target/batch-blob-delete-1.0.0-shaded.jar`.

> **Note:** Building requires access to Maven Central to download the Azure SDK dependencies.

## Running

After building, run the application with:

```bash
java -jar target/batch-blob-delete-1.0.0-shaded.jar [path-to-config]
```

If no argument is provided, `config/application.properties` is used.

The application logs progress, successes, and failures to the console using Log4j 2. Logs can be redirected or reconfigured by editing `src/main/resources/log4j2.xml`.
