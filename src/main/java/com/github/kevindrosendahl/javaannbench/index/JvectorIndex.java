package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

public class JvectorIndex {
  public static final class Builder implements Index.Builder {

    private final Path indexPath;
    private final RandomAccessVectorValues<float[]> vectors;
    private final GraphIndexBuilder<float[]> indexBuilder;
    private final int M;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final int numThreads;

    private Builder(
        Path indexPath,
        RandomAccessVectorValues<float[]> vectors,
        GraphIndexBuilder<float[]> indexBuilder,
        int M,
        int beamWidth,
        float neighborOverflow,
        float alpha,
        int numThreads) {
      this.indexPath = indexPath;
      this.vectors = vectors;
      this.indexBuilder = indexBuilder;
      this.M = M;
      this.beamWidth = beamWidth;
      this.neighborOverflow = neighborOverflow;
      this.alpha = alpha;
      this.numThreads = numThreads;
    }

    public static Index.Builder create(
        Path indexesPath,
        RandomAccessVectorValues<float[]> vectors,
        SimilarityFunction similarityFunction,
        int M,
        int beamWidth,
        float neighborOverflow,
        float alpha,
        int numThreads) {
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
              M,
              beamWidth,
              neighborOverflow,
              alpha);

      var path = indexesPath.resolve(buildDescription(M, beamWidth, neighborOverflow, alpha));
      return new JvectorIndex.Builder(
          path, vectors, indexBuilder, M, beamWidth, neighborOverflow, alpha, numThreads);
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
      return buildDescription(this.M, this.beamWidth, this.neighborOverflow, this.alpha);
    }

    @Override
    public long size() throws IOException {
      return 0;
    }

    @Override
    public void close() throws Exception {}

    private static String buildDescription(
        int M, int beamWidth, float neighborOverflow, float alpha) {
      return String.format(
          "jvector_vamana_M:%s-beamWidth:%s-neighborOverflow:%s-alpha:%s",
          M, beamWidth, neighborOverflow, alpha);
    }
  }
}
