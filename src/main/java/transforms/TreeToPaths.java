package transforms;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class TreeToPaths extends PTransform<PCollection<JsonNode>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<JsonNode> input) {
        return input
            .apply(ParDo.of(new ExtractPathsFromTree()));
    }

    public static class ExtractPathsFromTree extends DoFn<JsonNode, String> {

        @ProcessElement
        public void processElement(@Element JsonNode root, OutputReceiver<String> receiver) {
            getPaths(root, "", receiver);
        }
    }

    private static void getPaths(JsonNode node, String currentPath, DoFn.OutputReceiver<String> out) {

        currentPath
            += (node.get("Id").toString() + ","
            + node.get("level").toString() + ","
            + node.get("label").toString()) + "\n";

        //check if leaf:
        if (node.path("children").isMissingNode()) {
            out.output(currentPath);
            return;
        }

        // Else continue recursive traversal
        for (JsonNode child : node.get("children")) {
            getPaths(child, currentPath, out);
        }
    }
}
