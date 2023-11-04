package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
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

  public record HnswBuildParameters(int maxConn, int beamWidth, boolean scalarQuantization)
      implements BuildParameters {}

  public record VamanaBuildParameters(
      int maxConn, int beamWidth, float alpha, boolean scalarQuantization)
      implements BuildParameters {}

  public record QueryParameters(int numCandidates) {}

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
            case HNSW -> new Lucene99Codec() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new Lucene99HnswVectorsFormat(
                    ((HnswBuildParameters) buildParams).maxConn,
                    ((HnswBuildParameters) buildParams).beamWidth,
                    ((HnswBuildParameters) buildParams).scalarQuantization
                        ? new Lucene99ScalarQuantizedVectorsFormat()
                        : null);
              }
            };
            case SANDBOX_VAMANA -> new Lucene99Codec() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new VectorSandboxVamanaVectorsFormat(
                    ((VamanaBuildParameters) buildParams).maxConn,
                    ((VamanaBuildParameters) buildParams).beamWidth,
                    ((VamanaBuildParameters) buildParams).alpha,
                    ((VamanaBuildParameters) buildParams).scalarQuantization
                        ? new VectorSandboxScalarQuantizedVectorsFormat()
                        : null);
              }
            };
          };
      var writer =
          new IndexWriter(
              directory,
              new IndexWriterConfig()
                  .setCodec(codec)
                  .setUseCompoundFile(false)
                  .setRAMBufferSizeMB(20 * 1024));

      return new LuceneIndex.Builder(vectors, directory, writer, provider, buildParams, similarity);
    }

    @Override
    public BuildSummary build() throws IOException {
      var size = this.vectors.size();

      var buildStart = Instant.now();
      try (var progress = ProgressBar.create("building", size)) {
        var doc = new Document();

        for (int i = 0; i < this.vectors.size(); i++) {
          doc.clear();
          doc.add(new StoredField(ID_FIELD, i));
          doc.add(
              new KnnFloatVectorField(
                  VECTOR_FIELD, this.vectors.vectorValue(i), this.similarityFunction));
          this.writer.addDocument(doc);
          progress.inc();
        }
      }
      var buildEnd = Instant.now();

      var commitStart = Instant.now();
      this.writer.commit();
      var commitEnd = Instant.now();

      return new BuildSummary(
          List.of(
              new BuildPhase("build", Duration.between(buildStart, buildEnd)),
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
            "maxConn:%s-beamWidth:%s-scalarQuantization:%s",
            hnsw.maxConn, hnsw.beamWidth, hnsw.scalarQuantization);
        case VamanaBuildParameters vamana -> String.format(
            "maxConn:%s-beamWidth:%s-alpha:%s-scalarQuantization:%s",
            vamana.maxConn, vamana.beamWidth, vamana.alpha, vamana.scalarQuantization);
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
      var queryParams =
          Records.fromMap(parameters.queryParameters(), QueryParameters.class, "query parameters");

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
    public List<Integer> query(float[] vector, int k) throws IOException {
      var query = new KnnFloatVectorQuery(VECTOR_FIELD, vector, queryParams.numCandidates);
      var results = this.searcher.search(query, queryParams.numCandidates);
      var ids = new ArrayList<Integer>(k);

      for (int i = 0; i < k; i++) {
        var result = results.scoreDocs[i];
        var id =
            this.searcher
                .storedFields()
                .document(result.doc)
                .getField(ID_FIELD)
                .numericValue()
                .intValue();
        ids.add(id);
      }

      return ids;
    }

    @Override
    public String description() {
      return String.format(
          "lucene_%s_%s_numCandidates:%s",
          provider.description,
          LuceneIndex.Builder.buildParamString(buildParams),
          queryParams.numCandidates);
    }

    @Override
    public void close() throws Exception {
      this.directory.close();
      this.reader.close();
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
}
