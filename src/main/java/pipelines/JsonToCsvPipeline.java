

package pipelines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import options.InOutOptions;
import pojos.Person;
import transforms.PersonToCsvRow;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonToCsvPipeline {

    public static void main(String... args) throws IOException {

        PipelineOptionsFactory.register(InOutOptions.class);
        InOutOptions options = PipelineOptionsFactory.as(InOutOptions.class);

        options.setJobName("Parse Json to CSV");
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Read source JSON file.", TextIO.read().from(options.getInput()))
            .apply("Parse to POJO matching schema", ParseJsons.of(Person.class))
            .setCoder(SerializableCoder.of(Person.class))
            .apply("Create comma delimited string", new PersonToCsvRow())
            .apply("Write out to file", TextIO.write().to(options.getOutput())
                .withoutSharding());

        pipeline.run().waitUntilFinish();
    }


}

