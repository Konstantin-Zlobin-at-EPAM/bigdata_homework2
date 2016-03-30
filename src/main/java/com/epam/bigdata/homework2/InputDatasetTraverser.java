package com.epam.bigdata.homework2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class InputDatasetTraverser<TOUT> {

    final static Logger LOG = LoggerFactory.getLogger(InputDatasetTraverser.class);

    public static final int ID = 0;
    public static final int KeywordValue = 1;
    public static final int KeywordStatus = 2;
    public static final int PricingType = 3;
    public static final int KeywordMatchType = 4;
    public static final int DestinationURL = 5;
    public static final String HOMEWORK_INPUT_DATASET_FILE_NAME = "/apps/homework2/user.profile.tags.us.txt";

    public interface InputLineVisitor<TOUT> {
        Optional<TOUT> visitLine(String[] values, boolean isHeader);
    }

    public Stream<Optional<TOUT>> traverse(InputLineVisitor<TOUT> visitor) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            FileSystem fs = FileSystem.get(new YarnConfiguration());
            final BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(HOMEWORK_INPUT_DATASET_FILE_NAME))));
            String line;
            line = br.readLine();
            final List<Callable<Optional<TOUT>>> callables = new ArrayList<>();
            boolean headerLine = true;
            while (line != null) {
                LOG.debug("the next input line: " + line);
                final String[] values = line.split("\\t", -1);
                final boolean isHeader = headerLine;
                callables.add(() -> visitor.visitLine(values, isHeader));
                // be sure to read the next line otherwise you'll get an infinite loop
                line = br.readLine();
                headerLine = false;
            }
            final Stream<Optional<TOUT>> resultFutureStream = executor.invokeAll(callables).parallelStream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    return Optional.empty();
                }
            });
            executor.shutdown();
            return resultFutureStream;
        } catch (InterruptedException e) {
            LOG.error("total collapse of the task", e);
            return Stream.empty();
        }
    }
}
