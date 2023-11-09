package com.github.kevindrosendahl.javaannbench;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
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

  @Option(
      names = {"-c", "--config"},
      required = true)
  private String config;

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
    var datasetPath = workingDirectory.resolve("datasets");
    var indexesPath = workingDirectory.resolve("indexes");
    var reportsPath = workingDirectory.resolve("reports");

    if (this.build) {
      BuildBench.build(BuildSpec.load(Path.of(this.config)), datasetPath, indexesPath, reportsPath);
    }

    if (this.query) {
      QueryBench.test(QuerySpec.load(Path.of(this.config)), datasetPath, indexesPath, reportsPath);
    }
  }
}
