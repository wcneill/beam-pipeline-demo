
package pipelines;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import transforms.JSONTreeToPaths;
import transforms.PathsToCSV;

public class SequencePipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        options.
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Read Json", TextIO.read().from("src/main/resources/family_tree.json"))
            .apply("Traverse Json tree", new JSONTreeToPaths())
            .apply("Format tree paths", new PathsToCSV())
            .apply("Write to CSV", TextIO.write().to("src/main/resources/paths.csv")
                .withoutSharding());

        pipeline.run().waitUntilFinish();

    }
}

