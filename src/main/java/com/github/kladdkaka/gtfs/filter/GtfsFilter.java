package com.github.kladdkaka.gtfs.filter;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class GtfsFilter {

    public static final Set<String> blacklist = Set.of(
            "logging"
    );

    public static final Map<String, CheckedBiConsumer<Path, Path, IOException>> filterHandlers = Map.of(
            "feed_info.txt", GtfsFilter::handleFeedInfo
    );


    public static void main(String[] args) throws IOException {
        String inputFile = args[0]; //"C:\\Users\\Xenu\\Downloads\\gtfs.zip";
        String outputFile = args[1]; //"C:\\Users\\Xenu\\Downloads\\gtfs-out.zip";

        FileSystem fileSystem = FileSystems.newFileSystem(Path.of(inputFile));

        Path tempDirectory = Files.createTempDirectory("gtfs-temp-");
        tempDirectory.toFile().deleteOnExit();

        for (Path root : fileSystem.getRootDirectories()) {
            Files.walk(root)
                    .filter(Files::isRegularFile)
                    .filter(isBlacklisted().negate())
                    .forEach(path -> {
                        String fileName = extractFileName(path);

                        System.out.println("handling file " + fileName);

                        Path outPath = tempDirectory.resolve(fileName);
                        outPath.toFile().deleteOnExit();

                        CheckedBiConsumer<Path, Path, IOException> handler = filterHandlers.getOrDefault(fileName, GtfsFilter::handleDefault);

                        try {
                            handler.accept(path, outPath);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        Path outputPath = Files.createFile(Path.of(outputFile));

        System.out.println("begin zip");

        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(outputPath))) {
            Files.walk(tempDirectory)
                    .filter(Files::isRegularFile)
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(tempDirectory.relativize(path).toString());
                        try {
                            zs.putNextEntry(zipEntry);
                            Files.copy(path, zs);
                            zs.closeEntry();
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    });
        }
    }


    public static void handleFeedInfo(Path inputPath, Path outputPath) throws IOException {
        CsvParserSettings settings = getParserSettings();

        settings.excludeFields("conv_rev", "plan_rev");

        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(Files.newInputStream(inputPath));

        CsvWriter writer = createWriter(outputPath);

        writer.writeHeaders(parser.getRecordMetadata().selectedHeaders());

        Record record;
        while ((record = parser.parseNextRecord()) != null) {
            writer.writeRecord(record);
        }

        writer.close();
    }

    private static void handleDefault(Path inputPath, Path outputPath) throws IOException {
        CsvParserSettings settings = getParserSettings();

        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(Files.newInputStream(inputPath));

        CsvWriter writer = createWriter(outputPath);

        writer.writeHeaders(parser.getRecordMetadata().selectedHeaders());

        Record record;
        while ((record = parser.parseNextRecord()) != null) {
            writer.writeRecord(record);
        }

        writer.close();
    }

    private static CsvParserSettings getParserSettings() {
        CsvParserSettings settings = new CsvParserSettings();

        settings.getFormat().setLineSeparator("\n");
        settings.setHeaderExtractionEnabled(true);
        settings.trimValues(true);

        return settings;
    }

    private static CsvWriter createWriter(Path outputPath) throws IOException {
        return new CsvWriter(new BufferedWriter(new FileWriter(outputPath.toFile())), new CsvWriterSettings());
    }

    private static Predicate<Path> isBlacklisted() {
        return path -> blacklist.contains(path.getFileName().toString());
    }

    private static String extractFileName(Path path) {
        return path.getFileName().toString();
    }

}
