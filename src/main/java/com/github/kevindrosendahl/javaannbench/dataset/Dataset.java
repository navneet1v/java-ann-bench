package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.util.MMapRandomAccessVectorValues;
import java.util.List;

public record Dataset(
    String name,
    SimilarityFunction similarityFunction,
    int dimensions,
    MMapRandomAccessVectorValues train,
    MMapRandomAccessVectorValues test,
    List<List<Integer>> groundTruth) {}
