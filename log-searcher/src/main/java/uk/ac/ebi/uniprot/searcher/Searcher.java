package uk.ac.ebi.uniprot.searcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created 27/03/19
 *
 * @author Edd
 */
public class Searcher {
    private static final Pattern IP_IN_LOG_LINE_PATTERN = Pattern.compile("([0-9]+(\\.[0-9]+)+)\\s.*");
    private static final String ACCESS_LOG_PREFIX = "access_";
    private static final Map<String, Long> IP_COUNT_MAP = new HashMap<>();
    private static final int MAP_STATS_TO_SHOW = 20;
    private static final Pattern INVALID_GET_LINE_PATTERN = Pattern.compile("\\w+\\.(png|css)");
    private static long hitCount = 0L;

    public static void main(String[] args) {
        // Searcher DATE_FROM DATE_TO PATTERN LOG_DIR
        if (args.length != 4) {
            throw new IllegalArgumentException("Required usage parameters: DATE_FROM DATE_TO PATTERN LOG_DIR");
        }

        LocalDate dateFrom = LocalDate.parse(args[0], DateTimeFormatter.BASIC_ISO_DATE);
        LocalDate dateTo = LocalDate.parse(args[1], DateTimeFormatter.BASIC_ISO_DATE);
        Instant dateFromInstant = dateFrom.atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant dateToInstant = dateTo.atStartOfDay().toInstant(ZoneOffset.UTC);

        Pattern regex = Pattern.compile(args[2]);
        String logDir = args[3];

        if (!Files.exists(Paths.get(logDir))) {
            throw new IllegalArgumentException("Log directory does not exist " + logDir);
        }

        log("=====================================");
        log("Pattern = ", args[2]);
        log("Date from = ", args[0]);
        log("Date to = ", args[1]);
        log("Log dir = ", args[3]);
        log("-------------------------------------");

        search(Paths.get(logDir), regex, dateFromInstant, dateToInstant);
        printStats();
    }

    private static void printStats() {
        log("Total hits:", "\t", Long.toString(hitCount));
        log("Top", Integer.toString(MAP_STATS_TO_SHOW), "IPs:");
        IP_COUNT_MAP.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(MAP_STATS_TO_SHOW)
                .forEach(entry -> log(entry.getKey(), "\t", entry.getValue().toString()));
    }

    private static void search(Path logDir, final Pattern regex, final Instant dateFrom, final Instant dateTo) {
        try {
            Files.list(logDir)
                    .forEach(vmLogDir -> processVmLogDir(vmLogDir, regex, dateFrom, dateTo));
        } catch (IOException e) {
            throw new IllegalStateException("Could not list log directory, " + logDir);
        }
    }

    private static void processVmLogDir(Path vmLogDir, Pattern regex, final Instant dateFrom, final Instant dateTo) {
        try {
            Files.list(vmLogDir)
                    .filter(logFile -> logFile.getFileName().toString().startsWith(ACCESS_LOG_PREFIX))
                    .filter(logFile -> isBetweenDates(logFile, dateFrom, dateTo))
                    .forEach(logFile -> processFile(logFile, regex));
        } catch (IOException e) {
            throw new IllegalStateException("Could not list VM log directory, " + vmLogDir);
        }
    }

    private static void processFile(Path logFile, Pattern regex) {
        try (BufferedReader br = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            for (String line = null; (line = br.readLine()) != null; ) {
                if (isGetLine(line)) {
                    Matcher lineMatcher = regex.matcher(line);
                    if (lineMatcher.find()) {
                        Matcher ipMatcher = IP_IN_LOG_LINE_PATTERN.matcher(line);
                        if (ipMatcher.find()) {
                            IP_COUNT_MAP.compute(ipMatcher.group(1), (key, value) -> (value == null) ? 1 : value + 1);
                        }
                    }
                    hitCount++;
                }
            }
        } catch (IOException e) {
            log("Error:", e.getMessage());
        }
    }

    private static boolean isGetLine(String line) {
        Matcher matcher = INVALID_GET_LINE_PATTERN.matcher(line);
        return !matcher.find();
    }

    private static boolean isBetweenDates(Path logFile, Instant dateFrom, Instant dateTo) {
        try {
            Instant lastModified = Files.getLastModifiedTime(logFile).toInstant();
            return dateFrom.isBefore(lastModified) && dateTo.isAfter(lastModified);
        } catch (IOException e) {
            log("Error:", "Could not get last modified time of file,", logFile.toString());
            return false;
        }
    }

    private static void log(String... messages) {
        for (String message : messages) {
            System.out.print(message + " ");
        }
        System.out.println();
    }
}
