# java-ann-bench

Benchmarking framework for comapring various Java based vector index configurations.

## Dependencies
`java-ann-bench` requires Java 21 and [just](https://github.com/casey/just).

## Running
First, build an index by running
```
$ just build <dataset> <index-spec>
```

For example:
```
$ just build glove-100-angular lucene_hnsw_maxConn:16-beamWidth:100
WARNING: Using incubator modules: jdk.incubator.vector
building 100% │██████████████████████████│ 1183514/1183514 (0:10:33 / 0:00:00)
completed building index for lucene_hnsw_maxConn:16-beamWidth:100
        build phase: PT10M33.561425S
        commit phase: PT3.025441S
        total time: PT10M36.586866S
        size: 529.2 MiB
```

Then, run a query benchmark against a built index:
```
$ just query <dataset> <index-spec> <k>
```

For example:
```
$ just build glove-100-angular lucene_hnsw_maxConn:16-beamWidth:100_numCandidates:100 10
WARNING: Using incubator modules: jdk.incubator.vector
warmup 100% │████████████████████████████████│ 20000/20000 (0:00:21 / 0:00:00)
testing 100% │███████████████████████████████│ 30000/30000 (0:00:33 / 0:00:00)
completed recall test for lucene_sandbox-vamana_maxConn:32-beamWidth:100-alpha:1.2-scalarQuantization:false_numCandidates:100:
        average recall 0.82553
        average duration PT0.000940756S
        average minor faults 2.506233333333161
        average major faults 0.0
        max duration PT0.043382S
        max minor faults 2176.0
        max major faults 0.0
        total minor faults 75187.0
        total major faults 0.0
```
