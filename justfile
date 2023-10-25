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
  git -C submodules/lucene pull origin ann-sandbox
  cd submodules/lucene && ./gradlew jar
  mkdir -p libs
  cp submodules/lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar libs/

build dataset index:
    @./gradlew run --console=plain --quiet --args="--build --dataset={{dataset}} --index={{index}}"

query dataset index k:
    @./gradlew run --console=plain --quiet --args="--query --k={{k}} --dataset={{dataset}} --index={{index}}"
