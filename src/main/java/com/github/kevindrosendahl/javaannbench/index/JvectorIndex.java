package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Records;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import oshi.SystemInfo;

public class JvectorIndex {

  public record BuildParameters(int M, int beamWidth, float neighborOverflow, float alpha) {}

  public static final class Builder implements Index.Builder {

    private final Path indexPath;
    private final RandomAccessVectorValues<float[]> vectors;
    private final GraphIndexBuilder<float[]> indexBuilder;
    private final BuildParameters buildParams;
    private final int numThreads;

    private Builder(
        Path indexPath,
        RandomAccessVectorValues<float[]> vectors,
        GraphIndexBuilder<float[]> indexBuilder,
        BuildParameters buildParams,
        int numThreads) {
      this.indexPath = indexPath;
      this.vectors = vectors;
      this.indexBuilder = indexBuilder;
      this.buildParams = buildParams;
      this.numThreads = numThreads;
    }

    public static Index.Builder create(
        Path indexesPath,
        RandomAccessVectorValues<float[]> vectors,
        SimilarityFunction similarityFunction,
        Parameters parameters) {
      Preconditions.checkArgument(
          parameters.type().equals("vamana"),
          "unexpected jvector index type: %s",
          parameters.type());

      var buildParams =
          Records.fromMap(parameters.buildParameters(), BuildParameters.class, "build parameters");

      var vectorSimilarityFunction =
          switch (similarityFunction) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
          };

      var indexBuilder =
          new GraphIndexBuilder<>(
              vectors,
              VectorEncoding.FLOAT32,
              vectorSimilarityFunction,
              buildParams.M,
              buildParams.beamWidth,
              buildParams.neighborOverflow,
              buildParams.alpha);

      var numThreads =
          Optional.ofNullable(System.getenv("JVECTOR_NUM_THREADS"))
              .map(Integer::parseInt)
              .orElseGet(
                  () -> new SystemInfo().getHardware().getProcessor().getPhysicalProcessorCount());

      var path = indexesPath.resolve(buildDescription(buildParams));
      return new JvectorIndex.Builder(path, vectors, indexBuilder, buildParams, numThreads);
    }

    @Override
    public BuildSummary build() throws IOException {
      var pool = new ForkJoinPool(this.numThreads);
      var size = this.vectors.size();

      var buildStart = Instant.now();
      try (var progress = ProgressBar.create("building", size)) {
        IntStream.range(0, size)
            .parallel()
            .forEach(
                i -> {
                  this.indexBuilder.addGraphNode(i, this.vectors);
                  progress.inc();
                });
      }

      this.indexBuilder.cleanup();
      var buildEnd = Instant.now();

      var graph = this.indexBuilder.getGraph();

      var commitStart = Instant.now();
      try (var output = new DataOutputStream(new FileOutputStream(this.indexPath.toFile()))) {
        OnDiskGraphIndex.write(graph, vectors, output);
      }
      var commitEnd = Instant.now();

      return new BuildSummary(
          Duration.between(buildStart, buildEnd), Duration.between(commitStart, commitEnd));
    }

    @Override
    public String description() {
      return buildDescription(this.buildParams);
    }

    @Override
    public Bytes size() throws IOException {
      return Bytes.ofBytes(Files.size(this.indexPath));
    }

    @Override
    public void close() throws Exception {}

    private static String buildDescription(BuildParameters buildParams) {
      return String.format(
          "jvector_vamana_M:%s-beamWidth:%s-neighborOverflow:%s-alpha:%s",
          buildParams.M, buildParams.beamWidth, buildParams.neighborOverflow, buildParams.alpha);
    }
  }
}
