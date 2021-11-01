package coders;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.commons.compress.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonNodeCoder extends CustomCoder<JsonNode> {

    @Override
    public void encode(JsonNode node, OutputStream outStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String nodeString = mapper.writeValueAsString(node);
        outStream.write(nodeString.getBytes());
    }

    @Override
    public JsonNode decode(InputStream inStream) throws IOException {
        byte[] bytes = IOUtils.toByteArray(inStream);
        ObjectMapper mapper = new ObjectMapper();
        String json = new String(bytes);
        return mapper.readTree(json);
    }
}
