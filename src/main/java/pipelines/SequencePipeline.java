package pipelines;

import options.InOutOptions;
import org.apache.beam.repackaged.direct_java.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import transforms.StringToTree;
import transforms.TreeToPaths;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SequencePipeline {

    public static void main(String... args) throws IOException {

        PipelineOptionsFactory.register(InOutOptions.class);
        InOutOptions options = PipelineOptionsFactory.fromArgs(args).as(InOutOptions.class);

        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String json = readAndConvertJson(options.getInput());
        pipeline
            .apply(Create.of(json))
            .apply("Create JSON tree model", new StringToTree())
            .apply("Traverse JSON tree model", new TreeToPaths())
            .apply("Write paths to CSV", TextIO.write().to(options.getOutput()));
        pipeline.run().waitUntilFinish();

        saveDotString(pipeline, "src/main/resources/dot_string.svg");
    }

    private static String readAndConvertJson(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        String json = new String(encoded, Charset.defaultCharset());
        return convertToNdJson(json);
    }

    private static String convertToNdJson(String json) {
        return json.replaceAll("\\R", " ");
    }

    public static void saveDotString(Pipeline p, String filePath) {

        Path saveTo = FileSystems.getDefault().getPath(filePath);
        String dot = PipelineDotRenderer.toDotString(p);
        try {
            byte[] bytes = dot.getBytes("UTF-8");
            Files.write(saveTo, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

