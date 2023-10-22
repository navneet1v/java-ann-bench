package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.SimilarityFunction;
import com.github.kevindrosendahl.javaannbench.index.LuceneHnswIndex.HnswProvider;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.VectorSimilarityFunction;

public sealed interface Index permits LuceneHnswIndex {

  String description();

  static Index fromDescription(Path path, SimilarityFunction similarityFunction,
      String description) throws IOException {
    var parts = description.split("-");
    return switch (parts[0]) {
      case "lucene" -> fromLuceneDescription(path, similarityFunction, parts);
      default -> throw new RuntimeException("unknown index type: " + parts[0]);
    };
  }

  private static LuceneHnswIndex fromLuceneDescription(Path path,
      SimilarityFunction similarityFunction, String[] parts) throws IOException {
    Preconditions.checkArgument(parts.length == 5,
        "expected 4 arguments for lucene index description, got %s", parts.length);
    Preconditions.checkArgument(parts[1].equals("hnsw"), "unexpected lucene index type %s",
        parts[1]);

    var provider = switch (parts[2]) {
      case "lucene95" -> HnswProvider.LUCENE_95;
      case "sandbox" -> HnswProvider.SANDBOX;
      default -> throw new AssertionError(String.format("unexpected lucene codec %s", parts[2]));
    };

    var mPart = parts[3];
    var mSplit = mPart.split(":");
    Preconditions.checkArgument(mSplit.length == 2, "unexpected format for m parameter");
    Preconditions.checkArgument(mSplit[0].equals("M"), "expected M parameter first, got %s",
        mSplit[0]);
    var m = Integer.parseInt(mSplit[1]);

    var efConstructionPart = parts[3];
    var efConstructionSplit = efConstructionPart.split(":");
    Preconditions.checkArgument(efConstructionSplit.length == 2,
        "unexpected format for m parameter");
    Preconditions.checkArgument(efConstructionSplit[0].equals("efConstruction"),
        "expected M parameter first, got %s", mSplit[0]);
    var efConstruction = Integer.parseInt(efConstructionSplit[1]);

    return LuceneHnswIndex.create(path, provider, m, efConstruction, similarityFunction);
  }
}
