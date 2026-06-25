package ams.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolved configuration for one bombarder run.
 *
 * <p>Each setting is resolved from, in order of precedence:
 * <ol>
 *   <li>a CLI flag, e.g. {@code --scale=5}</li>
 *   <li>an environment variable, e.g. {@code SCALE=5} (handy for multiple instances)</li>
 *   <li>a {@code .env} file in the working directory (same keys as the CLI flags)</li>
 *   <li>the built-in default</li>
 * </ol>
 */
record BombarderConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String topic,
        double scale,
        int maxBurst,
        long intervalMs,
        long count,
        long durationSec) {

    private static final String DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9093";
    private static final String DEFAULT_SCHEMA_REGISTRY = "http://localhost:8081";
    private static final String DEFAULT_TOPIC = "accident.events";

    static boolean wantsHelp(String[] args) {
        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                return true;
            }
        }
        return false;
    }

    static BombarderConfig from(String[] args) {
        Map<String, String> cli = parseCli(args);
        Map<String, String> dotenv = loadDotEnv();

        String unknown = cli.keySet().stream()
                .filter(k -> !KNOWN_KEYS.contains(k))
                .findFirst().orElse(null);
        if (unknown != null) {
            throw new IllegalArgumentException("unknown option --" + unknown);
        }

        return new BombarderConfig(
                resolve(cli, dotenv, "bootstrap-servers", "BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                resolve(cli, dotenv, "schema-registry-url", "SCHEMA_REGISTRY_URL", DEFAULT_SCHEMA_REGISTRY),
                resolve(cli, dotenv, "topic", "TOPIC", DEFAULT_TOPIC),
                positiveDouble("scale", resolve(cli, dotenv, "scale", "SCALE", "1")),
                positiveInt("max-burst", resolve(cli, dotenv, "max-burst", "MAX_BURST", "100")),
                nonNegativeLong("interval-ms", resolve(cli, dotenv, "interval-ms", "INTERVAL_MS", "1000")),
                nonNegativeLong("count", resolve(cli, dotenv, "count", "COUNT", "0")),
                nonNegativeLong("duration-sec", resolve(cli, dotenv, "duration-sec", "DURATION_SEC", "0")));
    }

    String describe() {
        return "bootstrap=%s schema-registry=%s topic=%s scale=%s max-burst=%d interval-ms=%d count=%s duration-sec=%s"
                .formatted(bootstrapServers, schemaRegistryUrl, topic, scale, maxBurst, intervalMs,
                        count == 0 ? "unlimited" : count, durationSec == 0 ? "unlimited" : durationSec);
    }

    static void printHelp() {
        System.out.println("""
                stream-bombarder - load generator for the AMS pipeline.

                Produces bursts of random accident events to the source topic until stopped
                (Ctrl-C) or until --count / --duration-sec is reached. Run several instances
                in parallel to increase load.

                Usage:
                  java -jar stream-bombarder.jar [options]

                Options (CLI flag / env var / .env key):
                  --bootstrap-servers=HOST:PORT,...   BOOTSTRAP_SERVERS     Kafka brokers          (default localhost:9092,localhost:9093)
                  --schema-registry-url=URL           SCHEMA_REGISTRY_URL   Confluent Schema Reg.  (default http://localhost:8081)
                  --topic=NAME                         TOPIC                 Target topic           (default accident.events)
                  --scale=N                            SCALE                 Burst-size multiplier  (default 1)
                  --max-burst=N                        MAX_BURST             Max events per burst   (default 100, before scale)
                  --interval-ms=N                      INTERVAL_MS           Max delay between bursts, ms (default 1000)
                  --count=N                            COUNT                 Stop after N events    (default 0 = unlimited)
                  --duration-sec=N                     DURATION_SEC          Stop after N seconds   (default 0 = unlimited)
                  --help, -h                                                 Show this help

                Precedence: CLI flag > environment variable > .env file > default.

                Examples:
                  java -jar stream-bombarder.jar --scale=5
                  java -jar stream-bombarder.jar --count=10000 --max-burst=200
                  SCALE=10 java -jar stream-bombarder.jar --topic=accident.events
                """);
    }

    // ---- resolution helpers ----

    private static final List<String> KNOWN_KEYS = List.of(
            "bootstrap-servers", "schema-registry-url", "topic", "scale",
            "max-burst", "interval-ms", "count", "duration-sec", "help");

    private static String resolve(Map<String, String> cli, Map<String, String> dotenv,
                                  String cliKey, String envKey, String def) {
        if (cli.containsKey(cliKey)) {
            return cli.get(cliKey);
        }
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) {
            return env;
        }
        if (dotenv.containsKey(cliKey)) {
            return dotenv.get(cliKey);
        }
        if (dotenv.containsKey(envKey)) {
            return dotenv.get(envKey);
        }
        return def;
    }

    private static Map<String, String> parseCli(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            if (eq < 0) {
                map.put(body, "true");
            } else {
                map.put(body.substring(0, eq), body.substring(eq + 1));
            }
        }
        return map;
    }

    private static Map<String, String> loadDotEnv() {
        Map<String, String> map = new HashMap<>();
        Path path = Path.of(".env");
        if (!Files.isRegularFile(path)) {
            return map;
        }
        try {
            for (String line : Files.readAllLines(path)) {
                String trimmed = line.strip();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int eq = trimmed.indexOf('=');
                if (eq > 0) {
                    map.put(trimmed.substring(0, eq).strip(), trimmed.substring(eq + 1).strip());
                }
            }
        } catch (IOException e) {
            System.err.println("warning: could not read .env: " + e.getMessage());
        }
        return map;
    }

    private static double positiveDouble(String name, String value) {
        double parsed = parseDouble(name, value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be > 0, got: " + value);
        }
        return parsed;
    }

    private static int positiveInt(String name, String value) {
        long parsed = parseLong(name, value);
        if (parsed < 1 || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be >= 1, got: " + value);
        }
        return (int) parsed;
    }

    private static long nonNegativeLong(String name, String value) {
        long parsed = parseLong(name, value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must be >= 0, got: " + value);
        }
        return parsed;
    }

    private static double parseDouble(String name, String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number, got: " + value);
        }
    }

    private static long parseLong(String name, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number, got: " + value);
        }
    }
}
