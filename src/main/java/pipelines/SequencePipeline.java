
package pipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import transforms.JSONTreeToPaths;
import transforms.PathsToCSV;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SequencePipeline {

    public static void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String json = readAndConvertJson("src/main/resources/family_tree.json");

        pipeline
            .apply(Create.of(json))
            .apply("Traverse Json tree", new JSONTreeToPaths())
            .apply("Format tree paths", new PathsToCSV())
            .apply("Write to CSV", TextIO.write().to("src/main/resources/paths.csv")
                .withoutSharding());

        pipeline.run().waitUntilFinish();

    }

    private static String readAndConvertJson(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        String json = new String(encoded, Charset.defaultCharset());
        return convertToNdJson(json);
    }

    private static String convertToNdJson(String json) {
        return json.replaceAll("\\R", " ");
    }


}

