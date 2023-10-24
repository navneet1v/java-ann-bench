package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.bench.Recall;
import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.jline.terminal.TerminalBuilder;
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

  //  @Option(names = {"-d", "--dataset"}, required = true)
  @Option(names = {"-d", "--dataset"})
  private String dataset;

  //  @Option(names = {"-i", "--index"}, required = true)
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
    var start = Instant.now();
    try (var index = Index.Builder.fromDescription(dataset, indexesPath, this.index)) {
      try (var progress = ProgressBar.create("build", dataset.train().size())) {
        for (var vector : dataset.train()) {
          index.add(vector);
          progress.inc();
        }

      }

      var endAddVectors = Instant.now();
      LOGGER.info("finished adding vectors in {}, committing index",
          Duration.between(start, endAddVectors));

      index.commit();

      var end = Instant.now();
      LOGGER.info("finished committing index in {}", Duration.between(endAddVectors, end));

      LOGGER.info("completed building index for {}: total time {}, total size {}",
          index.description(), Duration.between(start, end), Bytes.ofBytes(index.size()));
    }
  }

  private void query(Dataset dataset, Path indexesPath) throws Exception {
    Preconditions.checkArgument(this.k != 0, "must supply k if running query");

    try (var index = Index.Querier.fromDescription(dataset, indexesPath, this.index)) {
      var result = Recall.test(index, dataset.test(), this.k, dataset.groundTruth());

      LOGGER.info("completed recall test for {}:", index.description());
      LOGGER.info("\taverage recall {}", result.recall().getMean());
      LOGGER.info("\taverage duration {}",
          Duration.ofNanos((long) result.executionDurationMicros().getMean()));
      LOGGER.info("\taverage minor faults {}", result.minorFaults().getMean());
      LOGGER.info("\taverage major faults {}", result.majorFaults().getMean());
      LOGGER.info("\tmax duration {}",
          Duration.ofNanos((long) result.executionDurationMicros().getMax()));
      LOGGER.info("\tmax minor faults {}", result.minorFaults().getMax());
      LOGGER.info("\tmax major faults {}", result.majorFaults().getMax());
    }
  }

  private Dataset dataset(Path datasetPath) throws IOException, InterruptedException {
    return Dataset.fromDescription(datasetPath, this.dataset);
  }
}
