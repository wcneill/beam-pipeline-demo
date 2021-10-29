
package transforms;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class JSONTreeToPaths extends PTransform<PCollection<String>, PCollection<List<String>>> {

    public static class ExtractPathsFromTree extends SimpleFunction<JsonNode, List<String>> {
        public List<String> apply(JsonNode root) {
            List<String> pathContainer = new ArrayList<>();
            getPaths(root, "", pathContainer);
            return pathContainer;
        }
    }

    public static class GetRootNode extends SimpleFunction<String, JsonNode> {
        public JsonNode apply(String jsonString) {
            try {
                return getRoot(jsonString);
            } catch (JsonProcessingException e) {
               e.printStackTrace();
               return null;
            }
        }
    }

    @Override
    public PCollection<List<String>> expand(PCollection<String> input) {
        return input
            .apply(MapElements.via(new GetRootNode()))
            .apply(MapElements.via(new ExtractPathsFromTree()))
            .setCoder(SerializableCoder.of(List<String>.class));
    }

    private static JsonNode getRoot(String jsonString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    private static void getPaths(JsonNode node, String currentPath, List<String> paths) {
        //check if leaf:
        if (node.path("children").isMissingNode()) {
            currentPath += node.get("Id");
            paths.add(currentPath);
            System.out.println(currentPath);
            return;
        }

        // recursively iterate over children
        currentPath += (node.get("Id") + ",");
        for (JsonNode child : node.get("children")) {
            getPaths(child, currentPath, paths);
        }
    }
}

