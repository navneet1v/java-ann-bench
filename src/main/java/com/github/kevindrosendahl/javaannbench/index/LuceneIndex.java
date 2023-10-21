package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;
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

public class LuceneIndex implements AutoCloseable {

  private static final String VECTOR_FIELD = "vector";

  private final Directory directory;
  private final Codec codec;
  private final VectorSimilarityFunction similarityFunction;
  private boolean built;
  private boolean closed;
  private Optional<IndexReader> reader;
  private Optional<IndexSearcher> searcher;

  private LuceneIndex(Directory directory, Codec codec,
      VectorSimilarityFunction similarityFunction) {
    this.directory = directory;
    this.codec = codec;
    this.similarityFunction = similarityFunction;
    this.built = false;
    this.reader = Optional.empty();
    this.searcher = Optional.empty();
  }

  public static LuceneIndex createLucene95(Path path, int maxConn, int beamWidth,
      SimilarityFunction similarityFunction)
      throws IOException {
    return create(path, new Lucene95Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene95HnswVectorsFormat(maxConn, beamWidth);
      }
    }, similarityFunction);
  }

  public static LuceneIndex createSandbox(Path path, int maxConn, int beamWidth,
      SimilarityFunction similarityFunction)
      throws IOException {
    var codec = new Lucene95Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new VectorSandboxHnswVectorsFormat(maxConn, beamWidth);
      }
    };
    return create(path, codec, similarityFunction);
  }

  private static LuceneIndex create(Path path, Codec codec, SimilarityFunction similarityFunction)
      throws IOException {

    var similarity = switch (similarityFunction) {
      case COSINE -> VectorSimilarityFunction.COSINE;
      case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
      case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
    };
    var directory = new MMapDirectory(path);
    return new LuceneIndex(directory, codec, similarity);
  }

  public void build(List<float[]> vectors) throws IOException {
    Preconditions.checkArgument(!this.built, "index is already built");

    try (var writer = new IndexWriter(this.directory, new IndexWriterConfig().setCodec(codec))) {
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
  public void close() throws Exception {
    Preconditions.checkArgument(!this.closed, "already closed");

    this.directory.close();
    if (this.reader.isPresent()) {
      this.reader.get().close();
    }
    this.closed = true;
  }
}
