package com.example.batchdelete;

import com.example.batchdelete.config.AppConfig;
import com.example.batchdelete.service.BatchDeletionResult;
import com.example.batchdelete.service.BlobBatchDeletionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class BatchBlobDeleteApplication {

    static {
        configureLogging();
    }

    private static final Logger LOGGER = LogManager.getLogger(BatchBlobDeleteApplication.class);

    private BatchBlobDeleteApplication() {
    }

    public static void main(String[] args) {
        boolean singleResponseRequested = CliArguments.hasSingleResponseFlag(args);
        CliArguments cliArguments;
        try {
            cliArguments = CliArguments.parse(args);
        } catch (IllegalArgumentException ex) {
            if (!singleResponseRequested) {
                System.err.println("Error: " + ex.getMessage());
                CliArguments.printUsage();
            }
            System.exit(singleResponseRequested ? 0 : 1);
            return;
        }

        try {
            if (cliArguments.help()) {
                CliArguments.printUsage();
                return;
            }

            boolean singleResponse = cliArguments.singleResponse();
            Path configPath = cliArguments.configPath().orElseGet(() -> Path.of("config", "application.properties"));
            AppConfig config = AppConfig.load(configPath, cliArguments.inputFilePath().orElse(null),
                    cliArguments.inputCsvData().orElse(null), cliArguments.batchSize().orElse(null),
                    cliArguments.threadCount().orElse(null), cliArguments.snapshotEnabled().orElse(null));

            LOGGER.info("Starting Batch Blob Delete application with {}",
                    config.getInputFilePath().<CharSequence>map(path -> "input file " + path)
                            .orElse("inline CSV content"));

            BlobBatchDeletionService service = new BlobBatchDeletionService(config);
            BatchDeletionResult result = service.execute();
            int exitCode;
            if (!singleResponse) {
                result.failureMessages().forEach(System.out::println);
                result.successMessages().forEach(System.out::println);
            }
            if (result.failureCount() > 0) {
                LOGGER.error("Batch completed with {} failures", result.failureCount());
                exitCode = singleResponse ? 0 : 1;
            } else {
                exitCode = singleResponse ? 1 : 0;
            }
            if (singleResponse || exitCode != 0) {
                System.exit(exitCode);
            }
        } catch (Exception ex) {
            LOGGER.fatal("Application failed", ex);
            System.exit(cliArguments.singleResponse() ? 0 : 1);
        }
    }

    private static void configureLogging() {
        if (System.getProperty("log4j.configurationFile") != null) {
            return;
        }

        Path configDir = Path.of("config");
        Path[] candidates = {
                configDir.resolve("log4j2.properties"),
                configDir.resolve("log4j2.xml")
        };

        for (Path candidate : candidates) {
            if (Files.isRegularFile(candidate)) {
                System.setProperty("log4j.configurationFile", candidate.toUri().toString());
                return;
            }
        }
    }

    private record CliArguments(Optional<Path> configPath, Optional<String> inputFilePath, Optional<String> inputCsvData,
            Optional<Integer> batchSize, Optional<Integer> threadCount, Optional<Boolean> snapshotEnabled,
            boolean singleResponse, boolean help) {

        private static CliArguments parse(String[] args) {
            if (args == null || args.length == 0) {
                return new CliArguments(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(), false, false);
            }

            Path configPath = null;
            String inputFile = null;
            String inputCsv = null;
            Integer batchSize = null;
            Integer threadCount = null;
            Boolean snapshotEnabled = null;
            boolean singleResponse = false;
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
                        inputCsv = unescapeCsvContent(requireValue(arg, args, ++i));
                        break;
                    case "--batch-size":
                    case "-b":
                        batchSize = parsePositiveInt(requireValue(arg, args, ++i), "batch size", 256);
                        break;
                    case "--threads":
                    case "-t":
                        threadCount = parsePositiveInt(requireValue(arg, args, ++i), "thread count", null);
                        break;
                    case "--snapshot-enabled":
                    case "-s":
                        snapshotEnabled = parseBoolean(requireValue(arg, args, ++i), "snapshot enabled");
                        break;
                    case "--single-response":
                    case "-sr":
                        singleResponse = true;
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
                    Optional.ofNullable(snapshotEnabled), singleResponse, help);
        }

        static boolean hasSingleResponseFlag(String[] args) {
            if (args == null) {
                return false;
            }
            for (String arg : args) {
                if ("--single-response".equals(arg) || "-sr".equals(arg)) {
                    return true;
                }
            }
            return false;
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

        private static boolean parseBoolean(String rawValue, String description) {
            String normalizedValue = rawValue == null ? null : rawValue.trim().toLowerCase();
            if (normalizedValue == null || normalizedValue.isEmpty()) {
                throw new IllegalArgumentException("Invalid value for " + description + ": '" + rawValue + "'");
            }
            if (normalizedValue.equals("true") || normalizedValue.equals("false")) {
                return Boolean.parseBoolean(normalizedValue);
            }
            throw new IllegalArgumentException("Invalid value for " + description + ": '" + rawValue + "'");
        }

        private static String unescapeCsvContent(String rawValue) {
            if (rawValue == null || rawValue.isEmpty()) {
                return rawValue;
            }

            StringBuilder builder = new StringBuilder(rawValue.length());
            boolean escaping = false;
            for (int i = 0; i < rawValue.length(); i++) {
                char current = rawValue.charAt(i);
                if (escaping) {
                    switch (current) {
                        case 'n':
                            builder.append('\n');
                            break;
                        case 'r':
                            builder.append('\r');
                            break;
                        case 't':
                            builder.append('\t');
                            break;
                        case '\\':
                            builder.append('\\');
                            break;
                        default:
                            builder.append('\\').append(current);
                            break;
                    }
                    escaping = false;
                } else if (current == '\\') {
                    escaping = true;
                } else {
                    builder.append(current);
                }
            }

            if (escaping) {
                builder.append('\\');
            }

            return builder.toString();
        }

        static void printUsage() {
            String usage = "Usage: java -jar batch-blob-delete.jar [options]\n" +
                    "Options:\n" +
                    "  -c, --config <path>       Path to configuration file (default: config/application.properties)\n" +
                    "  -f, --input-file <path>   Override input CSV file path\n" +
                    "  -d, --input-data <csv>    Provide inline CSV data (mutually exclusive with --input-file)\n" +
                    "  -b, --batch-size <size>   Override batch size (max 256)\n" +
                    "  -t, --threads <count>     Override thread pool size\n" +
                    "  -s, --snapshot-enabled <true|false>\n" +
                    "                             Override snapshot creation before delete\n" +
                    "  -sr, --single-response    Exit with 1 on success and 0 on failure, suppressing output\n" +
                    "  -h, --help                Show this help message";
            System.out.println(usage);
        }

        private static boolean isBlank(String value) {
            return value == null || value.isBlank();
        }
    }
}
