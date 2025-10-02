package com.example.batchdelete;

import com.example.batchdelete.config.AppConfig;
import com.example.batchdelete.service.BlobBatchDeletionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Optional;

public final class BatchBlobDeleteApplication {

    private static final Logger LOGGER = LogManager.getLogger(BatchBlobDeleteApplication.class);

    private BatchBlobDeleteApplication() {
    }

    public static void main(String[] args) {
        CliArguments cliArguments;
        try {
            cliArguments = CliArguments.parse(args);
        } catch (IllegalArgumentException ex) {
            System.err.println("Error: " + ex.getMessage());
            CliArguments.printUsage();
            System.exit(1);
            return;
        }

        try {
            if (cliArguments.help()) {
                CliArguments.printUsage();
                return;
            }

            Path configPath = cliArguments.configPath().orElseGet(() -> Path.of("config", "application.properties"));
            AppConfig config = AppConfig.load(configPath, cliArguments.inputFilePath().orElse(null),
                    cliArguments.inputCsvData().orElse(null), cliArguments.batchSize().orElse(null),
                    cliArguments.threadCount().orElse(null));

            LOGGER.info("Starting Batch Blob Delete application with {}",
                    config.getInputFilePath().<CharSequence>map(path -> "input file " + path)
                            .orElse("inline CSV content"));

            BlobBatchDeletionService service = new BlobBatchDeletionService(config);
            service.execute();
        } catch (Exception ex) {
            LOGGER.fatal("Application failed", ex);
            System.exit(1);
        }
    }

    private record CliArguments(Optional<Path> configPath, Optional<String> inputFilePath, Optional<String> inputCsvData,
            Optional<Integer> batchSize, Optional<Integer> threadCount, boolean help) {

        private static CliArguments parse(String[] args) {
            if (args == null || args.length == 0) {
                return new CliArguments(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.empty(), false);
            }

            Path configPath = null;
            String inputFile = null;
            String inputCsv = null;
            Integer batchSize = null;
            Integer threadCount = null;
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--help":
                    case "-h":
                        help = true;
                        break;
                    case "--config":
                    case "-c":
                        configPath = Path.of(requireValue(arg, args, ++i));
                        break;
                    case "--input-file":
                    case "-f":
                        inputFile = requireValue(arg, args, ++i);
                        break;
                    case "--input-data":
                    case "-d":
                        inputCsv = requireValue(arg, args, ++i);
                        break;
                    case "--batch-size":
                    case "-b":
                        batchSize = parsePositiveInt(requireValue(arg, args, ++i), "batch size", 256);
                        break;
                    case "--threads":
                    case "-t":
                        threadCount = parsePositiveInt(requireValue(arg, args, ++i), "thread count", null);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            if (!isBlank(inputFile) && !isBlank(inputCsv)) {
                throw new IllegalArgumentException("--input-file and --input-data cannot be used together");
            }

            return new CliArguments(Optional.ofNullable(configPath), Optional.ofNullable(inputFile),
                    Optional.ofNullable(inputCsv), Optional.ofNullable(batchSize), Optional.ofNullable(threadCount),
                    help);
        }

        private static String requireValue(String currentArg, String[] args, int valueIndex) {
            if (valueIndex >= args.length) {
                throw new IllegalArgumentException("Missing value for argument " + currentArg);
            }
            return args[valueIndex];
        }

        private static int parsePositiveInt(String rawValue, String description, Integer maxValue) {
            try {
                int parsedValue = Integer.parseInt(rawValue);
                if (parsedValue <= 0) {
                    throw new IllegalArgumentException(description + " must be greater than zero");
                }
                if (maxValue != null && parsedValue > maxValue) {
                    throw new IllegalArgumentException(
                            description + " must not exceed " + maxValue + ", but was " + parsedValue);
                }
                return parsedValue;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid value for " + description + ": '" + rawValue + "'", ex);
            }
        }

        static void printUsage() {
            String usage = "Usage: java -jar batch-blob-delete.jar [options]\n" +
                    "Options:\n" +
                    "  -c, --config <path>       Path to configuration file (default: config/application.properties)\n" +
                    "  -f, --input-file <path>   Override input CSV file path\n" +
                    "  -d, --input-data <csv>    Provide inline CSV data (mutually exclusive with --input-file)\n" +
                    "  -b, --batch-size <size>   Override batch size (max 256)\n" +
                    "  -t, --threads <count>     Override thread pool size\n" +
                    "  -h, --help                Show this help message";
            System.out.println(usage);
        }

        private static boolean isBlank(String value) {
            return value == null || value.isBlank();
        }
    }
}
