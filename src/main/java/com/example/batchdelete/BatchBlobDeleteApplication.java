package com.example.batchdelete;

import com.example.batchdelete.config.AppConfig;
import com.example.batchdelete.service.BlobBatchDeletionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;

public final class BatchBlobDeleteApplication {

    private static final Logger LOGGER = LogManager.getLogger(BatchBlobDeleteApplication.class);

    private BatchBlobDeleteApplication() {
    }

    public static void main(String[] args) {
        try {
            Path configPath = resolveConfigPath(args);
            AppConfig config = AppConfig.load(configPath);
            LOGGER.info("Starting Batch Blob Delete application with input file {}", config.getInputFilePath());

            BlobBatchDeletionService service = new BlobBatchDeletionService(config);
            service.execute();
        } catch (Exception ex) {
            LOGGER.fatal("Application failed", ex);
            System.exit(1);
        }
    }

    private static Path resolveConfigPath(String[] args) {
        if (args != null && args.length > 0) {
            return Path.of(args[0]);
        }
        return Path.of("config", "application.properties");
    }
}
