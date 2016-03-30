package com.epam.bigdata.homework2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.epam.bigdata.homework2.InputDatasetTraverser.*;
import static com.epam.bigdata.homework2.PagesDownloader.INTERMEDIATE_FILE_NAME;

public class WordsPerPageCounter implements InputDatasetTraverser.InputLineVisitor<String> {

    final static Logger LOG = LoggerFactory.getLogger(WordsPerPageCounter.class);

    final static private Path OUTPUT_FILE_PATH = new Path(HOMEWORK_INPUT_DATASET_FILE_NAME + "_out.txt");

    final private FileSystem fs;

    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new YarnConfiguration());
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(OUTPUT_FILE_PATH)));
        br.write(String.join("\n", (List<String>)
                new InputDatasetTraverser<String>().traverse(new WordsPerPageCounter(fs))
                        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList())));
    }

    public WordsPerPageCounter(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public Optional<String> visitLine(String[] values, boolean isHeader) {
        if (isHeader) {
            return Optional.of(String.join("\t", values));
        }
        String id = values[ID];
        final Path path = new Path(INTERMEDIATE_FILE_NAME + id);
        try {
            if (fs.getContentSummary(path).getLength() <= 0) {
                return Optional.empty();
            }
        } catch (IOException ioex) {
            return Optional.empty();
        }
        try {
            BufferedReader buffer = new BufferedReader(new InputStreamReader(fs.open(path)));
            List<Map.Entry<String, Long>> wordCounts =
                    buffer.lines().flatMap(s -> Arrays.asList(s.split("\\s+")).stream())
                            .map(s -> s.replaceAll("\\W", ""))
                            .filter(s -> !s.isEmpty() && s.length() > 3 && Character.isUpperCase(s.charAt(0)))
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                            .entrySet().stream().sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                            .limit(10).collect(Collectors.toList());
            LOG.debug("top 10 words for case id - " + id + ": " + wordCounts.stream()
                    .map(entry -> entry.getKey() + "\t" + entry.getValue()).collect(Collectors.toList()));
            final String keywordValue = String.join(", ", (List<String>) wordCounts.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
            return Optional.of(String.join("\t", values[ID], keywordValue, values[KeywordStatus],
                    values[PricingType], values[KeywordMatchType], values[DestinationURL]));
        } catch (Exception ex) {
            LOG.error("error while processing case id: " + id, ex);
            return Optional.empty();
        }

    }
}
