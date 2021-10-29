package transforms;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class PathsToCSV extends PTransform<PCollection<List<String>>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<List<String>> input) {
        return input.
            apply(MapElements.via(new ListToString()));
    }

    private static class ListToString extends SimpleFunction<List<String>, String> {
        public String apply(List<String> pathList) {
            StringBuilder sb = new StringBuilder();
            for (String row : pathList) {
                sb.append(row).append("\n");
            }
            return sb.toString();
        }
    }

}
