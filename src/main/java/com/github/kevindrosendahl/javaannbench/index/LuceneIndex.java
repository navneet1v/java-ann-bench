package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Exceptions;
import com.github.kevindrosendahl.javaannbench.util.Records;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.vectorsandbox.VectorSandboxScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.vectorsandbox.VectorSandboxVamanaVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public final class LuceneIndex {

  public enum Provider {
    HNSW("hnsw"),
    SANDBOX_VAMANA("sandbox-vamana");

    final String description;

    Provider(String description) {
      this.description = description;
    }

    static Provider parse(String description) {
      return switch (description) {
        case "hnsw" -> Provider.HNSW;
        case "sandbox-vamana" -> Provider.SANDBOX_VAMANA;
        default -> throw new RuntimeException("unexpected lucene index provider " + description);
      };
    }
  }

  public sealed interface BuildParameters permits VamanaBuildParameters, HnswBuildParameters {}

  public record HnswBuildParameters(
      int maxConn, int beamWidth, boolean scalarQuantization, int numThreads, boolean forceMerge)
      implements BuildParameters {}

  public record VamanaBuildParameters(
      int maxConn,
      int beamWidth,
      float alpha,
      int pqFactor,
      boolean inGraphVectors,
      boolean scalarQuantization,
      int numThreads,
      boolean forceMerge)
      implements BuildParameters {}

  public sealed interface QueryParameters permits HnswQueryParameters, VamanaQueryParameters {}

  public record HnswQueryParameters(int numCandidates) implements QueryParameters {}

  public record VamanaQueryParameters(
      int numCandidates,
      String pqRerank,
      boolean mlock,
      String parallelRerankThreads,
      int nodeCacheDegree)
      implements QueryParameters {}

  private static final String VECTOR_FIELD = "vector";
  private static final String ID_FIELD = "id";

  public static final class Builder implements Index.Builder {

    private final RandomAccessVectorValues<float[]> vectors;
    private final MMapDirectory directory;
    private final IndexWriter writer;
    private final Provider provider;
    private final BuildParameters buildParams;
    private final VectorSimilarityFunction similarityFunction;

    private Builder(
        RandomAccessVectorValues<float[]> vectors,
        MMapDirectory directory,
        IndexWriter writer,
        Provider provider,
        BuildParameters buildParams,
        VectorSimilarityFunction similarityFunction) {
      this.vectors = vectors;
      this.directory = directory;
      this.writer = writer;
      this.provider = provider;
      this.buildParams = buildParams;
      this.similarityFunction = similarityFunction;
    }

    public static Index.Builder create(
        Path indexesPath,
        RandomAccessVectorValues<float[]> vectors,
        SimilarityFunction similarityFunction,
        Parameters parameters)
        throws IOException {
      var provider = Provider.parse(parameters.type());

      var buildParams = parseBuildPrams(provider, parameters.buildParameters());

      var similarity =
          switch (similarityFunction) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
          };

      var description = buildDescription(provider, buildParams);
      var path = indexesPath.resolve(description);
      Preconditions.checkArgument(!path.toFile().exists(), "index already exists at %s", path);

      var directory = new MMapDirectory(path);

      var codec =
          switch (provider) {
            case HNSW -> {
              var hnswParams = (HnswBuildParameters) buildParams;
              yield new Lucene99Codec() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                  return new Lucene99HnswVectorsFormat(
                      hnswParams.maxConn,
                      hnswParams.beamWidth,
                      hnswParams.scalarQuantization
                          ? new Lucene99ScalarQuantizedVectorsFormat()
                          : null,
                      hnswParams.numThreads,
                      hnswParams.numThreads == 1
                          ? null
                          : Executors.newFixedThreadPool(hnswParams.numThreads));
                }
              };
            }
            case SANDBOX_VAMANA -> {
              var vamanaParams = (VamanaBuildParameters) buildParams;
              yield new Lucene99Codec() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                  return new VectorSandboxVamanaVectorsFormat(
                      vamanaParams.maxConn,
                      vamanaParams.beamWidth,
                      vamanaParams.alpha,
                      vamanaParams.pqFactor,
                      vamanaParams.inGraphVectors,
                      vamanaParams.scalarQuantization
                          ? new VectorSandboxScalarQuantizedVectorsFormat()
                          : null,
                      vamanaParams.forceMerge ? vamanaParams.numThreads : 1,
                      vamanaParams.numThreads == 1 || !vamanaParams.forceMerge
                          ? null
                          : Executors.newFixedThreadPool(vamanaParams.numThreads));
                }
              };
            }
          };
      var writer =
          new IndexWriter(
              directory,
              new IndexWriterConfig()
                  .setCodec(codec)
                  .setUseCompoundFile(false)
                  .setMaxBufferedDocs(1000000000)
                  .setRAMBufferSizeMB(40 * 1024)
                  .setMergePolicy(NoMergePolicy.INSTANCE)
                  .setMergeScheduler(new SerialMergeScheduler()));

      return new LuceneIndex.Builder(vectors, directory, writer, provider, buildParams, similarity);
    }

    @Override
    public BuildSummary build() throws IOException {
      var size = this.vectors.size();
      var numThreads =
          switch (buildParams) {
            case HnswBuildParameters params -> params.numThreads;
            case VamanaBuildParameters params -> params.numThreads;
          };

      var buildStart = Instant.now();
      try (var pool = new ForkJoinPool(numThreads)) {
        try (var progress = ProgressBar.create("building", size)) {
          pool.submit(
                  () -> {
                    IntStream.range(0, size)
                        .parallel()
                        .forEach(
                            i -> {
                              Exceptions.wrap(
                                  () -> {
                                    var doc = new Document();
                                    doc.add(new StoredField(ID_FIELD, i));
                                    doc.add(
                                        new KnnFloatVectorField(
                                            VECTOR_FIELD,
                                            this.vectors.vectorValue(i),
                                            this.similarityFunction));
                                    this.writer.addDocument(doc);
                                  });
                              progress.inc();
                            });
                  })
              .join();
        }
      }
      var buildEnd = Instant.now();

      var merge =
          switch (buildParams) {
            case HnswBuildParameters params -> params.forceMerge;
            case VamanaBuildParameters params -> params.forceMerge;
          };

      var mergeStart = Instant.now();
      if (merge) {
        this.writer.getConfig().setMergePolicy(new TieredMergePolicy());
        System.out.println("merging");
        this.writer.forceMerge(1);
      }
      var mergeEnd = Instant.now();

      System.out.println("committing");
      var commitStart = Instant.now();
      this.writer.commit();
      var commitEnd = Instant.now();

      return new BuildSummary(
          List.of(
              new BuildPhase("build", Duration.between(buildStart, buildEnd)),
              new BuildPhase("merge", Duration.between(mergeStart, mergeEnd)),
              new BuildPhase("commit", Duration.between(commitStart, commitEnd))));
    }

    public Bytes size() {
      return Bytes.ofBytes(FileUtils.sizeOfDirectory(this.directory.getDirectory().toFile()));
    }

    @Override
    public String description() {
      return buildDescription(this.provider, this.buildParams);
    }

    @Override
    public void close() throws Exception {
      this.writer.close();
      this.directory.close();
    }

    private static String buildDescription(Provider provider, BuildParameters params) {
      return String.format("lucene_%s_%s", provider.description, buildParamString(params));
    }

    private static String buildParamString(BuildParameters params) {
      return switch (params) {
        case HnswBuildParameters hnsw -> String.format(
            "maxConn:%s-beamWidth:%s-scalarQuantization:%s-numThreads:%s-forceMerge:%s",
            hnsw.maxConn,
            hnsw.beamWidth,
            hnsw.scalarQuantization,
            hnsw.numThreads,
            hnsw.forceMerge);
        case VamanaBuildParameters vamana -> String.format(
            "maxConn:%s-beamWidth:%s-alpha:%s-pqFactor:%s-inGraphVectors:%s-scalarQuantization:%s-numThreads:%s-forceMerge:%s",
            vamana.maxConn,
            vamana.beamWidth,
            vamana.alpha,
            vamana.pqFactor,
            vamana.inGraphVectors,
            vamana.scalarQuantization,
            vamana.numThreads,
            vamana.forceMerge);
      };
    }
  }

  public static final class Querier implements Index.Querier {

    private final Directory directory;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final Provider provider;
    private final BuildParameters buildParams;
    private final QueryParameters queryParams;

    private Querier(
        Directory directory,
        IndexReader reader,
        IndexSearcher searcher,
        Provider provider,
        BuildParameters buildParams,
        QueryParameters queryParams) {
      this.directory = directory;
      this.reader = reader;
      this.searcher = searcher;
      this.provider = provider;
      this.buildParams = buildParams;
      this.queryParams = queryParams;
    }

    public static Index.Querier create(Path indexesPath, Parameters parameters) throws IOException {
      var provider = Provider.parse(parameters.type());

      var buildParams = parseBuildPrams(provider, parameters.buildParameters());
      var queryParams = parseQueryPrams(provider, parameters.queryParameters());

      var buildDescription = LuceneIndex.Builder.buildDescription(provider, buildParams);
      var path = indexesPath.resolve(buildDescription);
      Preconditions.checkArgument(path.toFile().exists(), "index does not exist at {}", path);

      var directory = new MMapDirectory(indexesPath.resolve(buildDescription));
      var reader = DirectoryReader.open(directory);
      var searcher = new IndexSearcher(reader);
      return new LuceneIndex.Querier(
          directory, reader, searcher, provider, buildParams, queryParams);
    }

    @Override
    public List<Integer> query(float[] vector, int k, boolean ensureIds) throws IOException {
      var numCandidates =
          switch (queryParams) {
            case HnswQueryParameters hnsw -> hnsw.numCandidates;
            case VamanaQueryParameters vamana -> vamana.numCandidates;
          };

      var query = new KnnFloatVectorQuery(VECTOR_FIELD, vector, numCandidates);
      var results = this.searcher.search(query, numCandidates);
      var ids = new ArrayList<Integer>(k);

      for (int i = 0; i < k; i++) {
        var result = results.scoreDocs[i];
        var id =
            ensureIds
                ? this.searcher
                    .storedFields()
                    .document(result.doc)
                    .getField(ID_FIELD)
                    .numericValue()
                    .intValue()
                : result.doc;
        ids.add(id);
      }

      return ids;
    }

    @Override
    public String description() {
      return String.format(
          "lucene_%s_%s_%s",
          provider.description,
          LuceneIndex.Builder.buildParamString(buildParams),
          queryParamString());
    }

    @Override
    public void close() throws Exception {
      this.directory.close();
      this.reader.close();
    }

    private String queryParamString() {
      return switch (queryParams) {
        case HnswQueryParameters hnsw -> String.format("numCandidates:%s", hnsw.numCandidates);
        case VamanaQueryParameters vamana -> String.format(
            "numCandidates:%s-pqRerank:%s", vamana.numCandidates, vamana.pqRerank);
      };
    }
  }

  private static BuildParameters parseBuildPrams(
      Provider provider, Map<String, String> parameters) {
    return switch (provider) {
      case HNSW -> Records.fromMap(parameters, HnswBuildParameters.class, "build parameters");
      case SANDBOX_VAMANA -> Records.fromMap(
          parameters, VamanaBuildParameters.class, "build parameters");
    };
  }

  private static QueryParameters parseQueryPrams(
      Provider provider, Map<String, String> parameters) {
    return switch (provider) {
      case HNSW -> Records.fromMap(parameters, HnswQueryParameters.class, "query parameters");
      case SANDBOX_VAMANA -> Records.fromMap(
          parameters, VamanaQueryParameters.class, "query parameters");
    };
  }
}
