update-lucene:
  git -C submodules/lucene pull origin ann-sandbox
  cd submodules/lucene && ./gradlew jar
  mkdir -p libs
  cp submodules/lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar libs/

build dataset index:
    @./gradlew run --console=plain --quiet --args="--build --dataset={{dataset}} --index={{index}}"

query dataset index k:
    @./gradlew run --console=plain --quiet --args="--query --k={{k}} --dataset={{dataset}} --index={{index}}"
