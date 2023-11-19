heap_size := "40g"

init:
  git submodule init
  git submodule update
  @just update-jvector
  @just update-lucene

update-jvector:
  git -C submodules/jvector pull origin ann-sandbox
  cd submodules/jvector && ./mvnw clean package
  mkdir -p libs
  cp submodules/jvector/jvector-multirelease/target/jvector-1.0.3-SNAPSHOT.jar libs/

update-lucene:
  git -C submodules/lucene pull origin vamana2
  cd submodules/lucene && ./gradlew jar
  mkdir -p libs
  cp submodules/lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar libs/

build-docker:
  docker build --no-cache -t java-ann-bench .

build config:
  @./gradlew run --console=plain --quiet -PminHeapSize="-Xmx{{heap_size}}" -PmaxHeapSize=-"Xms{{heap_size}}" --args="--build --config={{config}}"

query config:
  #!/usr/bin/env bash
  set -exuo pipefail

  pq_rerank=$(yq e '.query.pqRerank' {{config}})
  export VAMANA_PQ_RERANK=${pq_rerank}
  ./gradlew run --console=plain --quiet -PminHeapSize="-Xmx{{heap_size}}" -PmaxHeapSize=-"Xms{{heap_size}}" --args="--query --config={{config}}"

query-docker config:
  #!/usr/bin/env bash
  set -exuo pipefail

  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  system_memory=$(yq e '.runtime.systemMemory' {{config}})
  docker run --rm \
    -v "$(pwd)/configs":/java-ann-bench/configs \
    -v "$(pwd)/datasets":/java-ann-bench/datasets \
    -v "$(pwd)/indexes":/java-ann-bench/indexes \
    -v "$(pwd)/reports":/java-ann-bench/reports \
    -m ${system_memory} \
    java-ann-bench \
    bash -c "git pull && just update-lucene && just query-docker-internal {{config}}"

query-docker-internal config:
  #!/usr/bin/env bash
  set -exuo pipefail

  pq_rerank=$(yq e '.query.pqRerank' {{config}})
  export VAMANA_PQ_RERANK=${pq_rerank}
  heap_size=$(yq e '.runtime.heapSize' {{config}})
  ./gradlew run --console=plain --quiet -PminHeapSize="-Xmx${heap_size}" -PmaxHeapSize=-"Xms${heap_size}" --args="--query --config={{config}}"


iouring:
  @./gradlew runIoUring