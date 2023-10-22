update-lucene:
  git -C submodules/lucene pull origin ann-sandbox
  cd submodules/lucene && ./gradlew jar
  mkdir -p libs
  cp submodules/lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar libs/

bench dataset index:
    @./gradlew run --console=plain --quiet --args="--build --query --dataset={{dataset}} --index={{index}}"
