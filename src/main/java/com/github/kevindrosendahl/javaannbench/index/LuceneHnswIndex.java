package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.vectorsandbox.VectorSandboxHnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public final class LuceneHnswIndex implements AutoCloseable, Index {

  public enum HnswProvider {
    LUCENE_95, SANDBOX,
  }

  private static final String VECTOR_FIELD = "vector";

  private final Directory directory;
  private final Codec codec;
  private final HnswProvider provider;
  private final int maxConn;
  private final int beamWidth;
  private final VectorSimilarityFunction similarityFunction;
  private boolean built;
  private boolean closed;
  private Optional<IndexReader> reader;
  private Optional<IndexSearcher> searcher;

  private LuceneHnswIndex(Directory directory, Codec codec, HnswProvider provider, int maxConn,
      int beamWidth, VectorSimilarityFunction similarityFunction) {
    this.directory = directory;
    this.codec = codec;
    this.provider = provider;
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.similarityFunction = similarityFunction;
    this.built = false;
    this.reader = Optional.empty();
    this.searcher = Optional.empty();
  }

  public static LuceneHnswIndex create(Path indexesPath, HnswProvider hnswProvider, int maxConn,
      int beamWidth, SimilarityFunction similarityFunction) throws IOException {

    var luceneCodec = switch (hnswProvider) {
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

    var similarity = switch (similarityFunction) {
      case COSINE -> VectorSimilarityFunction.COSINE;
      case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
      case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
    };

    var description = description(hnswProvider, maxConn, beamWidth);
    var directory = new MMapDirectory(indexesPath.resolve(description));
    return new LuceneHnswIndex(directory, luceneCodec, hnswProvider, maxConn, beamWidth,
        similarity);
  }

  public void build(List<float[]> vectors) throws IOException {
    Preconditions.checkArgument(!this.built, "index is already built");

    try (var writer = new IndexWriter(this.directory,
        new IndexWriterConfig().setCodec(codec).setRAMBufferSizeMB(2 * 1024))) {
      var doc = new Document();
      for (var vector : vectors) {
        doc.clear();
        doc.add(new KnnFloatVectorField(VECTOR_FIELD, vector, this.similarityFunction));
        writer.addDocument(doc);
      }
    }

    var reader = DirectoryReader.open(directory);
    this.reader = Optional.of(reader);
    var searcher = new IndexSearcher(reader);
    this.searcher = Optional.of(searcher);

    this.built = true;
  }

  public List<Integer> query(float[] vector, int k, int numCandidates) throws IOException {
    Preconditions.checkArgument(this.built, "index has not been built");

    var query = new KnnFloatVectorQuery(VECTOR_FIELD, vector, numCandidates);
    var results = this.searcher.get().search(query, numCandidates);
    return Arrays.stream(results.scoreDocs).map(scoreDoc -> scoreDoc.doc).limit(k).toList();
  }

  @Override
  public String description() {
    return description(this.provider, this.maxConn, this.beamWidth);
  }

  @Override
  public void close() throws Exception {
    Preconditions.checkArgument(!this.closed, "already closed");

    this.directory.close();
    if (this.reader.isPresent()) {
      this.reader.get().close();
    }
    this.closed = true;
  }

  private static String description(HnswProvider provider, int maxConn, int beamWidth) {
    var providerDescription = switch (provider) {
      case LUCENE_95 -> "lucene95";
      case SANDBOX -> "sandbox";
    };
    return String.format("lucene-hnsw-%s-M:%s-efConstruction:%s", providerDescription, maxConn,
        beamWidth);
  }
}
