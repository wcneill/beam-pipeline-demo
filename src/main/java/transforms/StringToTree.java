package transforms;

import coders.JsonNodeCoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class StringToTree extends PTransform<PCollection<String>, PCollection<JsonNode>> {

    @Override
    public PCollection<JsonNode> expand(PCollection<String> input) {
        return input
            .apply(MapElements.via(new ExtractRoot()))
            .setCoder(new JsonNodeCoder());
    }

    public static class ExtractRoot extends SimpleFunction<String, JsonNode> {
        public JsonNode apply(String jsonString) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = null;

            try {
                root = mapper.readTree(jsonString);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return root;
        }
    }
}
