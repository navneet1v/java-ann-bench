package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.bench.Recall;
import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.index.LuceneHnswIndex;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  @Option(names = {"-d", "--dataset"}, required = true)
  private String dataset;

  @Option(names = {"-i", "--index"}, required = true)
  private String[] indexes;

  public static void main(String[] args) {
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
    var workingDirectory = Path.of(System.getProperty("user.dir"));
    var datasetDirectory = workingDirectory.resolve("datasets");
    var indexesDirectory = workingDirectory.resolve("indexes");

    var dataset = dataset(datasetDirectory);
    var indexes = indexes(indexesDirectory, dataset.similarityFunction());

    if (this.build) {
      for (var index : indexes) {
        switch (index) {
          case LuceneHnswIndex luceneHnswIndex -> luceneHnswIndex.build(dataset.train());
        }
      }
    }

    if (this.query) {
      for (var index : indexes) {
        // FIXME: take in k and query parameters
        var result = switch (index) {
          case LuceneHnswIndex luceneHnswIndex ->
              Recall.test((vector, k) -> luceneHnswIndex.query(vector, k, k), dataset.test(), 10,
                  dataset.groundTruth());
        };

        LOGGER.info("completed recall test for {}: average recall {}, average duration {}",
            index.description(), result.recall(), result.averageExecutionTime());
      }
    }
  }

  private Dataset dataset(Path datasetPath) throws IOException, InterruptedException {
    return Dataset.fromDescription(datasetPath, this.dataset);
  }

  private List<Index> indexes(Path indexingPath, SimilarityFunction similarityFunction)
      throws IOException {
    Preconditions.checkArgument(this.indexes.length != 0, "must supply index descriptions");

    var indexes = new ArrayList<Index>(this.indexes.length);
    for (var description : this.indexes) {
      var index = Index.fromDescription(indexingPath, similarityFunction, description);
      indexes.add(index);
    }

    return indexes;
  }
}
