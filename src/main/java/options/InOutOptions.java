package options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface InOutOptions extends PipelineOptions {

    @Description("Input to the pipeline")
    @Default.String("src/main/resources/input")
    String getInput();
    void setInput(String input);

    @Description("Output from the pipeline")
    @Default.String("src/main/resources/output")
    String getOutput();
    void setOutput(String output);

}
