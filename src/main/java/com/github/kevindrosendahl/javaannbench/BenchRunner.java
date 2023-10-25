package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.bench.Recall;
import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "bench-runner", mixinStandardHelpOptions = true)
public class BenchRunner implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchRunner.class);

  @Option(names = {"-b", "--build"})
  private boolean build;

  @Option(names = {"-q", "--query"})
  private boolean query;

  @Option(names = {"-k", "--k"})
  private int k;

  @Option(names = {"-d", "--dataset"})
  private String dataset;

  @Option(names = {"-i", "--index"})
  private String index;

  public static void main(String[] args) {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    CommandLine cmd = new CommandLine(new BenchRunner());
    int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    try {
      throwableRun();
    } catch (Exception e) {
      LOGGER.error("caught exception during execution", e);
      throw new RuntimeException(e);
    }
  }

  private void throwableRun() throws Exception {
    Preconditions.checkArgument(!(this.build && this.query), "cannot build and query");
    Preconditions.checkArgument(this.build || this.query, "cannot build and query");

    var workingDirectory = Path.of(System.getProperty("user.dir"));
    var datasetDirectory = workingDirectory.resolve("datasets");
    var indexesPath = workingDirectory.resolve("indexes");

    var dataset = dataset(datasetDirectory);

    if (this.build) {
      build(dataset, indexesPath);
    }

    if (this.query) {
      query(dataset, indexesPath);
    }
  }

  private void build(Dataset dataset, Path indexesPath) throws Exception {
    try (var index = Index.Builder.fromDescription(dataset, indexesPath, this.index)) {
      var summary = index.build();
      LOGGER.info("completed building index for {}:");
      LOGGER.info("\tbuild phase: {}", summary.build());
      LOGGER.info("\tcommit phase: {}", summary.commit());
      LOGGER.info("\ttotal time: {}", summary.build().plus(summary.commit()));
      LOGGER.info("\tsize: {}", index.size());
    }
  }

  private void query(Dataset dataset, Path indexesPath) throws Exception {
    Preconditions.checkArgument(this.k != 0, "must supply k if running query");

    try (var index = Index.Querier.fromDescription(dataset, indexesPath, this.index)) {
      var result = Recall.test(index, dataset.test(), this.k, dataset.groundTruth());

      LOGGER.info("completed recall test for {}:", index.description());
      LOGGER.info("\taverage recall {}", result.recall().getMean());
      LOGGER.info(
          "\taverage duration {}",
          Duration.ofNanos((long) result.executionDurationMicros().getMean()));
      LOGGER.info("\taverage minor faults {}", result.minorFaults().getMean());
      LOGGER.info("\taverage major faults {}", result.majorFaults().getMean());
      LOGGER.info(
          "\tmax duration {}", Duration.ofNanos((long) result.executionDurationMicros().getMax()));
      LOGGER.info("\tmax minor faults {}", result.minorFaults().getMax());
      LOGGER.info("\tmax major faults {}", result.majorFaults().getMax());
    }
  }

  private Dataset dataset(Path datasetPath) throws IOException, InterruptedException {
    return Dataset.fromDescription(datasetPath, this.dataset);
  }
}
