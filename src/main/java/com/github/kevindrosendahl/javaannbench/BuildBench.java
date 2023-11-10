package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.index.Index.Builder.BuildPhase;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildBench {

  private static final Logger LOGGER = LoggerFactory.getLogger(BuildBench.class);

  public static void build(BuildSpec spec, Path datasetPath, Path indexesPath, Path reportsPath)
      throws Exception {
    var dataset = Datasets.load(datasetPath, spec.dataset());

    try (var index =
        Index.Builder.fromParameters(
            dataset, indexesPath, spec.provider(), spec.type(), spec.build())) {
      var summary = index.build();
      var totalTime =
          summary.phases().stream().map(BuildPhase::duration).reduce(Duration.ZERO, Duration::plus);

      LOGGER.info("completed building index for {}", index.description());
      summary
          .phases()
          .forEach(phase -> LOGGER.info("\t{} phase: {}", phase.description(), phase.duration()));
      LOGGER.info("\ttotal time: {}", totalTime);
      LOGGER.info("\tsize: {}", index.size());

      new Report(index.description(), spec, totalTime, summary.phases(), index.size())
          .write(reportsPath);
    }
  }

  private record Report(
      String indexDescription,
      BuildSpec spec,
      Duration total,
      List<BuildPhase> phases,
      Bytes size) {

    void write(Path reportsPath) throws Exception {
      var now = Instant.now().getEpochSecond();
      var path =
          reportsPath.resolve(
              String.format("%s-build-%s-%s", now, spec.dataset(), indexDescription));
      var data =
          new String[] {
            "v1",
            indexDescription,
            spec.dataset(),
            spec.provider(),
            spec.type(),
            spec.buildString(),
            Long.toString(total.toNanos()),
            phases.stream()
                .map(phase -> phase.description() + ":" + phase.duration().toNanos())
                .collect(Collectors.joining("-")),
            Long.toString(size.toBytes()),
          };

      try (var writer = Files.newBufferedWriter(path);
          var printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
        printer.printRecord((Object[]) data);
        printer.flush();
      }
    }
  }
}
