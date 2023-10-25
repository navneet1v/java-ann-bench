package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Records;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.SimpleMappedReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oshi.SystemInfo;

public class JVectorIndex {

  private static final String INDEX_FILE = "index.bin";

  public record BuildParameters(int M, int beamWidth, float neighborOverflow, float alpha) {}

  public record QueryParameters(int numCandidates) {}

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
        Parameters parameters)
        throws IOException {
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
      Files.createDirectories(path);

      return new JVectorIndex.Builder(path, vectors, indexBuilder, buildParams, numThreads);
    }

    @Override
    public BuildSummary build() throws IOException {
      var pool = new ForkJoinPool(this.numThreads);
      var size = this.vectors.size();

      var buildStart = Instant.now();
      try (var progress = ProgressBar.create("building", size)) {
        pool.submit(
            () -> {
              IntStream.range(0, size)
                  .parallel()
                  .forEach(
                      i -> {
                        this.indexBuilder.addGraphNode(i, this.vectors);
                        progress.inc();
                      });
            });
      }

      this.indexBuilder.cleanup();
      var buildEnd = Instant.now();

      var commitStart = Instant.now();
      try (var output =
          new DataOutputStream(new FileOutputStream(this.indexPath.resolve(INDEX_FILE).toFile()))) {
        var graph = this.indexBuilder.getGraph();
        OnDiskGraphIndex.write(graph, vectors, output);
      }
      var commitEnd = Instant.now();

      return new BuildSummary(
          List.of(
              new BuildPhase("build", Duration.between(buildStart, buildEnd)),
              new BuildPhase("commit", Duration.between(commitStart, commitEnd))));
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

  public static class Querier implements Index.Querier {
    private final ReaderSupplier readerSupplier;
    private final GraphIndex<float[]> graph;
    private final VectorSimilarityFunction similarityFunction;
    private final BuildParameters buildParams;
    private final QueryParameters queryParams;

    public Querier(
        ReaderSupplier readerSupplier,
        GraphIndex<float[]> graph,
        VectorSimilarityFunction similarityFunction,
        BuildParameters buildParams,
        QueryParameters queryParams) {
      this.readerSupplier = readerSupplier;
      this.graph = graph;
      this.similarityFunction = similarityFunction;
      this.buildParams = buildParams;
      this.queryParams = queryParams;
    }

    public static Index.Querier create(
        Path indexesPath, SimilarityFunction similarityFunction, Parameters parameters)
        throws IOException {
      Preconditions.checkArgument(
          parameters.type().equals("vamana"),
          "unexpected jvector index type: %s",
          parameters.type());

      var buildParams =
          Records.fromMap(parameters.buildParameters(), BuildParameters.class, "build parameters");
      var queryParams =
          Records.fromMap(parameters.queryParameters(), QueryParameters.class, "query parameters");

      var buildDescription = JVectorIndex.Builder.buildDescription(buildParams);
      var path = indexesPath.resolve(buildDescription).resolve(INDEX_FILE);
      Preconditions.checkArgument(path.toFile().exists(), "index does not exist at {}", path);

      var vectorSimilarityFunction =
          switch (similarityFunction) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
          };

      var readerSupplier = new SimpleMappedReaderSupplier(path);
      var onDiskGraph = new OnDiskGraphIndex<float[]>(readerSupplier, 0);
      var cachingGraph = new CachingGraphIndex(onDiskGraph);

      return new JVectorIndex.Querier(
          readerSupplier, cachingGraph, vectorSimilarityFunction, buildParams, queryParams);
    }

    @Override
    public List<Integer> query(float[] vector, int k) throws IOException {
      var view = this.graph.getView();

      NeighborSimilarity.ExactScoreFunction scoreFunction =
          i -> this.similarityFunction.compare(vector, view.getVector(i));

      var searcher = new GraphSearcher.Builder<>(view).build();
      var results = searcher.search(scoreFunction, null, queryParams.numCandidates, Bits.ALL);

      return Arrays.stream(results.getNodes())
          .map(nodeScore -> nodeScore.node)
          .limit(k)
          .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
      this.graph.close();
      this.readerSupplier.close();
    }

    @Override
    public String description() {
      return String.format(
          "jvector_vamana_M:%s-beamWidth:%s-neighborOverflow:%s-alpha:%s_numCandidates:%s",
          buildParams.M,
          buildParams.beamWidth,
          buildParams.neighborOverflow,
          buildParams.alpha,
          queryParams.numCandidates);
    }
  }
}
