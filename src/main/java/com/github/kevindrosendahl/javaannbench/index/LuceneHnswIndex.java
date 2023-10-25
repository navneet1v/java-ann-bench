package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.vectorsandbox.VectorSandboxHnswVectorsFormat;
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

public final class LuceneHnswIndex {

  public enum HnswProvider {
    LUCENE_95,
    SANDBOX;

    static HnswProvider parse(String description) {
      return switch (description) {
        case "hnsw-lucene95" -> HnswProvider.LUCENE_95;
        case "hnsw-sandbox" -> HnswProvider.SANDBOX;
        default -> throw new RuntimeException("unexpected lucene index provider " + description);
      };
    }
  }

  private static final String VECTOR_FIELD = "vector";
  private static final String ID_FIELD = "id";

  public static final class Builder implements Index.Builder {

    private final RandomAccessVectorValues<float[]> vectors;
    private final MMapDirectory directory;
    private final IndexWriter writer;
    private final HnswProvider provider;
    private final int maxConn;
    private final int beamWidth;
    private final VectorSimilarityFunction similarityFunction;

    private Builder(
        RandomAccessVectorValues<float[]> vectors,
        MMapDirectory directory,
        IndexWriter writer,
        HnswProvider provider,
        int maxConn,
        int beamWidth,
        VectorSimilarityFunction similarityFunction) {
      this.vectors = vectors;
      this.directory = directory;
      this.writer = writer;
      this.provider = provider;
      this.maxConn = maxConn;
      this.beamWidth = beamWidth;
      this.similarityFunction = similarityFunction;
    }

    public static Index.Builder create(
        Path indexesPath,
        RandomAccessVectorValues<float[]> vectors,
        SimilarityFunction similarityFunction,
        Parameters parameters)
        throws IOException {
      var provider = HnswProvider.parse(parameters.type());

      var buildParameters = parameters.buildParameters();
      Preconditions.checkArgument(
          buildParameters.size() == 2,
          "unexpected number of build parameters. expected 2, got %s",
          buildParameters.size());
      Preconditions.checkArgument(buildParameters.containsKey("M"), "must specify M");
      Preconditions.checkArgument(
          buildParameters.containsKey("efConstruction"), "must specify efConstruction");
      var maxConn = Integer.parseInt(buildParameters.get("M"));
      var beamWidth = Integer.parseInt(buildParameters.get("efConstruction"));

      var similarity =
          switch (similarityFunction) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
          };

      var description = buildDescription(provider, maxConn, beamWidth);
      var path = indexesPath.resolve(description);
      Preconditions.checkArgument(!path.toFile().exists(), "index already exists at %s", path);

      var directory = new MMapDirectory(path);

      var codec =
          switch (provider) {
            case LUCENE_95 -> new Lucene95Codec() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new Lucene95HnswVectorsFormat(maxConn, beamWidth);
              }
            };
            case SANDBOX -> new Lucene95Codec() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new VectorSandboxHnswVectorsFormat(maxConn, beamWidth);
              }
            };
          };
      var writer =
          new IndexWriter(
              directory, new IndexWriterConfig().setCodec(codec).setRAMBufferSizeMB(2 * 1024));

      return new LuceneHnswIndex.Builder(
          vectors, directory, writer, provider, maxConn, beamWidth, similarity);
    }

    @Override
    public BuildSummary build() throws IOException {
      var size = this.vectors.size();
      Duration build;

      try (var progress = ProgressBar.create("building", size)) {
        var doc = new Document();

        var buildStart = Instant.now();
        for (int i = 0; i < this.vectors.size(); i++) {
          doc.clear();
          doc.add(new StoredField(ID_FIELD, i));
          doc.add(
              new KnnFloatVectorField(
                  VECTOR_FIELD, this.vectors.vectorValue(i), this.similarityFunction));
          this.writer.addDocument(doc);
          progress.inc();
        }
        var buildEnd = Instant.now();
        build = Duration.between(buildStart, buildEnd);
      }

      var commitStart = Instant.now();
      this.writer.commit();
      var commitEnd = Instant.now();

      return new BuildSummary(build, Duration.between(commitStart, commitEnd));
    }

    public long size() {
      return FileUtils.sizeOfDirectory(this.directory.getDirectory().toFile());
    }

    @Override
    public String description() {
      return buildDescription(this.provider, this.maxConn, this.beamWidth);
    }

    @Override
    public void close() throws Exception {
      this.writer.close();
      this.directory.close();
    }

    private static String buildDescription(HnswProvider provider, int maxConn, int beamWidth) {
      var providerDescription =
          switch (provider) {
            case LUCENE_95 -> "lucene95";
            case SANDBOX -> "sandbox";
          };
      return String.format(
          "lucene_hnsw-%s_M:%s-efConstruction:%s", providerDescription, maxConn, beamWidth);
    }
  }

  public static final class Querier implements Index.Querier {

    private final Directory directory;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final HnswProvider provider;
    private final int maxConn;
    private final int beamWidth;
    private final int numCandidates;

    private Querier(
        Directory directory,
        IndexReader reader,
        IndexSearcher searcher,
        HnswProvider provider,
        int maxConn,
        int beamWidth,
        int numCandidates) {
      this.directory = directory;
      this.reader = reader;
      this.searcher = searcher;
      this.provider = provider;
      this.maxConn = maxConn;
      this.beamWidth = beamWidth;
      this.numCandidates = numCandidates;
    }

    public static Index.Querier create(Path indexesPath, Parameters parameters) throws IOException {
      var provider = HnswProvider.parse(parameters.type());

      var buildParameters = parameters.buildParameters();
      Preconditions.checkArgument(
          buildParameters.size() == 2,
          "unexpected number of build parameters. expected 2, got %s",
          buildParameters.size());
      Preconditions.checkArgument(buildParameters.containsKey("M"), "must specify M");
      Preconditions.checkArgument(
          buildParameters.containsKey("efConstruction"), "must specify efConstruction");
      var maxConn = Integer.parseInt(buildParameters.get("M"));
      var beamWidth = Integer.parseInt(buildParameters.get("efConstruction"));

      var queryParameters = parameters.queryParameters();
      Preconditions.checkArgument(
          queryParameters.size() == 1,
          "unexpected number of build parameters. expected 1, got %s",
          queryParameters.size());
      Preconditions.checkArgument(queryParameters.containsKey("efSearch"), "must specify efSearch");
      var numCandidates = Integer.parseInt(queryParameters.get("efSearch"));

      var buildDescription = LuceneHnswIndex.Builder.buildDescription(provider, maxConn, beamWidth);
      var path = indexesPath.resolve(buildDescription);
      Preconditions.checkArgument(path.toFile().exists(), "index does not exist at {}", path);

      var directory = new MMapDirectory(indexesPath.resolve(buildDescription));
      var reader = DirectoryReader.open(directory);
      var searcher = new IndexSearcher(reader);
      return new LuceneHnswIndex.Querier(
          directory, reader, searcher, provider, maxConn, beamWidth, numCandidates);
    }

    @Override
    public List<Integer> query(float[] vector, int k) throws IOException {
      var query = new KnnFloatVectorQuery(VECTOR_FIELD, vector, numCandidates);
      var results = this.searcher.search(query, numCandidates);
      var ids = new ArrayList<Integer>(k);

      for (var result : results.scoreDocs) {
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
      var providerDescription =
          switch (provider) {
            case LUCENE_95 -> "lucene95";
            case SANDBOX -> "sandbox";
          };
      return String.format(
          "lucene_hnsw-%s_M:%s-efConstruction:%s_efSearch:%s",
          providerDescription, maxConn, beamWidth, numCandidates);
    }

    @Override
    public void close() throws Exception {
      this.directory.close();
      this.reader.close();
    }
  }
}
