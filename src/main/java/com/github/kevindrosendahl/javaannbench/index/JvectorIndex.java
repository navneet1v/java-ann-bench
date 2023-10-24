package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

public class JvectorIndex {
  public static final class Builder implements Index.Builder {

    private final Path indexPath;
    private final RandomAccessVectorValues<float[]> vectors;
    private final GraphIndexBuilder<float[]> indexBuilder;
    private final int numThreads;

    private Builder(
        Path indexPath,
        RandomAccessVectorValues<float[]> vectors,
        GraphIndexBuilder<float[]> indexBuilder,
        int numThreads) {
      this.indexPath = indexPath;
      this.vectors = vectors;
      this.indexBuilder = indexBuilder;
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

      var path = indexesPath.resolve(buildDescription());
      return new JvectorIndex.Builder(path, vectors, indexBuilder, numThreads);
    }

    public void build() throws IOException {
      var executor =
          new ThreadPoolExecutor(
              this.numThreads,
              this.numThreads,
              0L,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(numThreads * 2),
              new CallerRunsPolicy());

      try (var progress = ProgressBar.create("building", this.vectors.size())) {
        for (int i = 0; i < this.vectors.size(); i++) {
          var id = i;
          CompletableFuture.runAsync(() -> this.indexBuilder.addGraphNode(id, this.vectors))
              .thenRun(progress::inc);
        }
      }

      executor.shutdown();
      try {
        Preconditions.checkArgument(executor.awaitTermination(1, TimeUnit.MINUTES));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      this.indexBuilder.cleanup();
      var graph = this.indexBuilder.getGraph();

      try (var output = new DataOutputStream(new FileOutputStream(this.indexPath.toFile()))) {
        OnDiskGraphIndex.write(graph, vectors, output);
      }
    }

    @Override
    public String description() {
      return buildDescription();
    }

    @Override
    public void add(int id, float[] vector) throws IOException {}

    @Override
    public void commit() throws IOException {}

    @Override
    public long size() throws IOException {
      return 0;
    }

    @Override
    public void close() throws Exception {}

    private static String buildDescription() {
      return "jvector_vamana";
    }
  }
}
