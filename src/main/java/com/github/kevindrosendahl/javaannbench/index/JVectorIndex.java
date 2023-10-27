package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Records;
import com.google.common.base.Preconditions;
import com.indeed.util.mmap.MMapBuffer;
import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.SimpleMappedReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex.View;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.NeighborSimilarity.ExactScoreFunction;
import io.github.jbellis.jvector.graph.NeighborSimilarity.ReRanker;
import io.github.jbellis.jvector.graph.NeighborSimilarity.ScoreFunction;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

public class JVectorIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(JVectorIndex.class);
  private static final String GRAPH_FILE = "graph.bin";
  private static final String COMPRESSED_VECTOR_FILE_FORMAT = "compressed-vectors-%s.bin";

  public record BuildParameters(int M, int beamWidth, float neighborOverflow, float alpha) {}

  public record QueryParameters(int numCandidates, int pqFactor) {}

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
                })
            .join();
      }

      this.indexBuilder.cleanup();
      var buildEnd = Instant.now();

      LOGGER.info("finished building index, committing");
      var commitStart = Instant.now();
      try (var output =
          new DataOutputStream(
              new BufferedOutputStream(
                  Files.newOutputStream(this.indexPath.resolve(GRAPH_FILE))))) {
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
      return Bytes.ofBytes(FileUtils.sizeOfDirectory(this.indexPath.toFile()));
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
    private final Optional<CompressedVectors> compressedVectors;
    private final VectorSimilarityFunction similarityFunction;
    private final BuildParameters buildParams;
    private final QueryParameters queryParams;

    public Querier(
        ReaderSupplier readerSupplier,
        GraphIndex<float[]> graph,
        Optional<CompressedVectors> compressedVectors,
        VectorSimilarityFunction similarityFunction,
        BuildParameters buildParams,
        QueryParameters queryParams) {
      this.readerSupplier = readerSupplier;
      this.graph = graph;
      this.compressedVectors = compressedVectors;
      this.similarityFunction = similarityFunction;
      this.buildParams = buildParams;
      this.queryParams = queryParams;
    }

    public static Index.Querier create(
        Path indexesPath,
        SimilarityFunction similarityFunction,
        int dimensions,
        Parameters parameters)
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
      var indexPath = indexesPath.resolve(buildDescription);
      var path = indexPath.resolve(GRAPH_FILE);
      Preconditions.checkArgument(path.toFile().exists(), "index does not exist at {}", path);

      var vectorSimilarityFunction =
          switch (similarityFunction) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
          };

      var readerSupplier = new MMapReaderSupplier(path);
      var onDiskGraph = new OnDiskGraphIndex<float[]>(readerSupplier, 0);
      var cachingGraph = new CachingGraphIndex(onDiskGraph);
      var compressedVectors =
          compressedVectors(
              indexPath, dimensions, queryParams.pqFactor, cachingGraph, vectorSimilarityFunction);

      return new JVectorIndex.Querier(
          readerSupplier,
          cachingGraph,
          compressedVectors,
          vectorSimilarityFunction,
          buildParams,
          queryParams);
    }

    @Override
    public List<Integer> query(float[] vector, int k) throws IOException {
      var view = this.graph.getView();

      NeighborSimilarity.ExactScoreFunction scoreFunction =
          i -> this.similarityFunction.compare(vector, view.getVector(i));

      var searcher = new GraphSearcher.Builder<>(view).build();
      var querySimilarity = querySimilarity(vector, view);
      var results =
          searcher.search(
              querySimilarity.scoreFunction,
              querySimilarity.reRanker,
              queryParams.numCandidates,
              Bits.ALL);

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
          "jvector_vamana_M:%s-beamWidth:%s-neighborOverflow:%s-alpha:%s_numCandidates:%s-pqFactor:%s",
          buildParams.M,
          buildParams.beamWidth,
          buildParams.neighborOverflow,
          buildParams.alpha,
          queryParams.numCandidates,
          queryParams.pqFactor);
    }

    private record QuerySimilarity(ScoreFunction scoreFunction, ReRanker<float[]> reRanker) {}

    private QuerySimilarity querySimilarity(float[] vector, View<float[]> view) {
      if (this.compressedVectors.isEmpty()) {
        return new QuerySimilarity(
            (ExactScoreFunction) (i -> this.similarityFunction.compare(vector, view.getVector(i))),
            null);
      }

      return new QuerySimilarity(
          compressedVectors.get().approximateScoreFunctionFor(vector, this.similarityFunction),
          (i, vectors) -> this.similarityFunction.compare(vector, vectors.get(i)));
    }

    private static RandomAccessVectorValues<float[]> graphViewVectors(
        View<float[]> view, int dimensions) {
      return new RandomAccessVectorValues<float[]>() {
        @Override
        public int size() {
          return view.size();
        }

        @Override
        public int dimension() {
          return dimensions;
        }

        @Override
        public float[] vectorValue(int i) {
          return view.getVector(i);
        }

        @Override
        public boolean isValueShared() {
          return false;
        }

        @Override
        public RandomAccessVectorValues<float[]> copy() {
          return this;
        }
      };
    }

    private static Optional<CompressedVectors> compressedVectors(
        Path indexPath,
        int dimensions,
        int pqFactor,
        GraphIndex<float[]> graph,
        VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {
      if (pqFactor <= 0) {
        return Optional.empty();
      }

      var compressedVectorsFile =
          indexPath.resolve(String.format(COMPRESSED_VECTOR_FILE_FORMAT, pqFactor));
      if (compressedVectorsFile.toFile().exists()) {
        try (var reader = new SimpleMappedReaderSupplier(compressedVectorsFile)) {
          return Optional.of(CompressedVectors.load(reader.get(), 0));
        }
      }

      var pqDims = dimensions / pqFactor;

      LOGGER.info(
          "index configured with pqFactor of {}, building codebook with {} dimensions",
          pqFactor,
          pqDims);

      var graphVectors = graphViewVectors(graph.getView(), dimensions);
      var size = graph.size();

      List<float[]> vectors;
      try (var progress = ProgressBar.create("loading vectors from graph", size)) {
        vectors =
            IntStream.range(0, size)
                .sequential()
                .mapToObj(graphVectors::vectorValue)
                .peek(ignored -> progress.inc())
                .collect(Collectors.toList());
      }

      LOGGER.info("building codebooks");
      var pqStart = Instant.now();
      var pq =
          ProductQuantization.compute(
              new ListRandomAccessVectorValues(vectors, dimensions),
              pqDims,
              vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN);
      var pqEnd = Instant.now();
      LOGGER.info("built codebooks in {}", Duration.between(pqStart, pqEnd));

      LOGGER.info("beginning to quantize vectors");
      var quantizeStart = Instant.now();
      var quantizedVectors = pq.encodeAll(vectors);
      var compressedVectors = new CompressedVectors(pq, quantizedVectors);
      var quantizeEnd = Instant.now();
      LOGGER.info(
          "quantized vectors in {}, compressed size: {}",
          Duration.between(quantizeStart, quantizeEnd),
          Bytes.ofBytes(compressedVectors.memorySize()));

      LOGGER.info("writing quantized vectors to disk");
      var writeStart = Instant.now();
      try (var output =
          new DataOutputStream(new FileOutputStream(compressedVectorsFile.toFile()))) {
        compressedVectors.write(output);
      }
      var writeEnd = Instant.now();
      LOGGER.info("wrote quantized vectors in {}", Duration.between(writeStart, writeEnd));

      return Optional.of(compressedVectors);
    }
  }

  private static class MMapReaderSupplier implements ReaderSupplier {
    private final MMapBuffer buffer;

    public MMapReaderSupplier(Path path) throws IOException {
      buffer = new MMapBuffer(path, FileChannel.MapMode.READ_ONLY, ByteOrder.BIG_ENDIAN);
    }

    @Override
    public RandomAccessReader get() {
      return new MMapReader(buffer);
    }

    @Override
    public void close() throws IOException {
      buffer.close();
    }
  }

  private static class MMapReader implements RandomAccessReader {
    private final MMapBuffer buffer;
    private long position;
    private byte[] floatsScratch = new byte[0];
    private byte[] intsScratch = new byte[0];

    MMapReader(MMapBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void seek(long offset) {
      position = offset;
    }

    public int readInt() {
      try {
        return buffer.memory().getInt(position);
      } finally {
        position += Integer.BYTES;
      }
    }

    public void readFully(byte[] bytes) {
      read(bytes, 0, bytes.length);
    }

    private void read(byte[] bytes, int offset, int count) {
      try {
        buffer.memory().getBytes(position, bytes, offset, count);
      } finally {
        position += count;
      }
    }

    @Override
    public void readFully(float[] floats) {
      int bytesToRead = floats.length * Float.BYTES;
      if (floatsScratch.length < bytesToRead) {
        floatsScratch = new byte[bytesToRead];
      }
      read(floatsScratch, 0, bytesToRead);
      ByteBuffer byteBuffer = ByteBuffer.wrap(floatsScratch).order(ByteOrder.BIG_ENDIAN);
      byteBuffer.asFloatBuffer().get(floats);
    }

    @Override
    public void read(int[] ints, int offset, int count) {
      int bytesToRead = (count - offset) * Integer.BYTES;
      if (intsScratch.length < bytesToRead) {
        intsScratch = new byte[bytesToRead];
      }
      read(intsScratch, 0, bytesToRead);
      ByteBuffer byteBuffer = ByteBuffer.wrap(intsScratch).order(ByteOrder.BIG_ENDIAN);
      byteBuffer.asIntBuffer().get(ints, offset, count);
    }

    @Override
    public void close() {
      // don't close buffer, let the Supplier handle that
    }
  }
}
