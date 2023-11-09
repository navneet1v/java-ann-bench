package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.bench.Recall;
import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.index.Index.Builder.BuildPhase;
import com.github.kevindrosendahl.javaannbench.util.Madvise.Advice;
import com.google.common.base.Preconditions;
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

  @Option(names = {"-c", "--config"})
  private String config;

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
    Preconditions.checkArgument(this.build || this.query, "must build or query");

    var workingDirectory = Path.of(System.getProperty("user.dir"));
    var datasetDirectory = workingDirectory.resolve("datasets");
    var indexesPath = workingDirectory.resolve("indexes");

    if (this.build) {
      build(datasetDirectory, indexesPath);
    }

    if (this.query) {
      query(datasetDirectory, indexesPath);
    }
  }

  private void build(Path datasetDirectory, Path indexesPath) throws Exception {
    var dataset = dataset(datasetDirectory);
    dataset.train().advise(Advice.WILLNEED);

    try (var index = builder(dataset, indexesPath)) {
      var summary = index.build();
      var totalTime =
          summary.phases().stream().map(BuildPhase::duration).reduce(Duration.ZERO, Duration::plus);

      LOGGER.info("completed building index for {}", index.description());
      summary
          .phases()
          .forEach(phase -> LOGGER.info("\t{} phase: {}", phase.description(), phase.duration()));
      LOGGER.info("\ttotal time: {}", totalTime);
      LOGGER.info("\tsize: {}", index.size());
    }
  }

  private void query(Path datasetDirectory, Path indexesPath) throws Exception {
    var dataset = dataset(datasetDirectory);
    dataset.test().advise(Advice.WILLNEED);

    try (var index = querier(dataset, indexesPath)) {
      var result = Recall.test(index, dataset.test(), k(), dataset.groundTruth());

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
      LOGGER.info("\ttotal minor faults {}", result.minorFaults().getSum());
      LOGGER.info("\ttotal major faults {}", result.majorFaults().getSum());
    }
  }

  private Dataset dataset(Path datasetsPath) throws Exception {
    var dataset = this.dataset;
    if (this.config != null) {
      if (this.build) {
        dataset = BuildSpec.load(Path.of(this.config)).dataset();
      } else {
        dataset = QuerySpec.load(Path.of(this.config)).dataset();
      }
    }

    return Datasets.load(datasetsPath, dataset);
  }

  private Index.Builder builder(Dataset dataset, Path indexesPath) throws Exception {
    if (this.config == null) {
      return Index.Builder.fromDescription(dataset, indexesPath, this.index);
    }

    var config = BuildSpec.load(Path.of(this.config));
    return Index.Builder.fromParameters(
        dataset, indexesPath, config.provider(), config.type(), config.buildParameters());
  }

  private Index.Querier querier(Dataset dataset, Path indexesPath) throws Exception {
    if (this.config == null) {
      return Index.Querier.fromDescription(dataset, indexesPath, this.index);
    }

    var config = QuerySpec.load(Path.of(this.config));
    return Index.Querier.fromParameters(
        dataset,
        indexesPath,
        config.provider(),
        config.type(),
        config.buildParameters(),
        config.queryParameters());
  }

  private int k() throws Exception {
    if (this.config == null) {
      return this.k;
    }

    return QuerySpec.load(Path.of(this.config)).k();
  }
}
