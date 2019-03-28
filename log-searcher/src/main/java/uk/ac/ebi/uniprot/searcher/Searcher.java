package uk.ac.ebi.uniprot.searcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
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

import static java.time.temporal.ChronoUnit.DAYS;

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
    private static final Pattern INVALID_LOG_LINE_PATTERN = Pattern.compile("\\w+\\.(png|css|rss|js)");
    private static final int ENTRY_COUNT_PER_DAY_THRESHOLD = 10;
    private static long hitCount = 0L;

    public static void main(String[] args) {
        // Searcher DATE_FROM DATE_TO PATTERN LOG_DIR COUNT_ONLY
        if (args.length != 5) {
            throw new IllegalArgumentException("Required usage parameters: DATE_FROM DATE_TO PATTERN LOG_DIR COUNT_ONLY");
        }

        LocalDate dateFrom = LocalDate.parse(args[0], DateTimeFormatter.BASIC_ISO_DATE);
        LocalDate dateTo = LocalDate.parse(args[1], DateTimeFormatter.BASIC_ISO_DATE);
        Instant dateFromInstant = dateFrom.atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant dateToInstant = dateTo.atStartOfDay().toInstant(ZoneOffset.UTC);

        long daysBetween = DAYS.between(dateFrom, dateTo);

        boolean countOnly = Boolean.parseBoolean(args[4]);

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
        log("Count only = ", args[4]);
        log("-------------------------------------");

        search(Paths.get(logDir), regex, dateFromInstant, dateToInstant, countOnly);
        printStats(countOnly, daysBetween);
    }

    private static void printStats(boolean countOnly, long daysBetween) {
        log("Total hits:", "\t", Long.toString(hitCount));
        if (!countOnly) {
            log("Top", Integer.toString(MAP_STATS_TO_SHOW), "IPs: (showing IPs requesting *on average* more than", Integer.toString(ENTRY_COUNT_PER_DAY_THRESHOLD), "times per day)");
            IP_COUNT_MAP.entrySet().stream()
                    .filter(entry -> entry.getValue() > daysBetween * ENTRY_COUNT_PER_DAY_THRESHOLD)
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(MAP_STATS_TO_SHOW)
                    .forEach(entry -> log(entry.getKey(), "\t", entry.getValue().toString()));
        }
    }

    private static void search(Path logDir, final Pattern regex, final Instant dateFrom, final Instant dateTo, boolean countOnly) {
        try {
            Files.list(logDir)
                    .forEach(vmLogDir -> processVmLogDir(vmLogDir, regex, dateFrom, dateTo, countOnly));
        } catch (IOException e) {
            throw new IllegalStateException("Could not list log directory, " + logDir);
        }
    }

    private static void processVmLogDir(Path vmLogDir, Pattern regex, final Instant dateFrom, final Instant dateTo, boolean countOnly) {
        try {
            Files.list(vmLogDir)
                    .filter(logFile -> logFile.getFileName().toString().startsWith(ACCESS_LOG_PREFIX))
                    .filter(logFile -> isBetweenDates(logFile, dateFrom, dateTo))
                    .forEach(logFile -> processFile(logFile, regex, countOnly));
        } catch (IOException e) {
            throw new IllegalStateException("Could not list VM log directory, " + vmLogDir);
        }
    }

    private static void processFile(Path logFile, Pattern regex, boolean countOnly) {
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        try (FileInputStream input = new FileInputStream(logFile.toFile());
             InputStreamReader reader = new InputStreamReader(input, decoder);
             BufferedReader br = new BufferedReader(reader)) {
            for (String line = null; (line = br.readLine()) != null; ) {
                String decodedLine = URLDecoder.decode(line, "UTF-8");
                if (isValidLine(decodedLine)) {
                    Matcher regexMatcher = regex.matcher(decodedLine);
                    if (regexMatcher.find()) {
                        if (!countOnly) {
                            Matcher ipMatcher = IP_IN_LOG_LINE_PATTERN.matcher(decodedLine);
                            if (ipMatcher.find()) {
                                IP_COUNT_MAP
                                        .compute(ipMatcher.group(1), (key, value) -> (value == null) ? 1 : value + 1);
                            }
                        }
                        hitCount++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log("Error:", e.getMessage());
        }
    }

    private static boolean isValidLine(String line) {
        return !INVALID_LOG_LINE_PATTERN.matcher(line).find();
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
