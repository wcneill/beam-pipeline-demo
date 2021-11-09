# beam-pipeline-demo
Demo of how to use Apache Beam to convert JSON input stream to rows in a CSV (or database).

Pipelines consist of a series of transforms. Each transform has an input type and output type. Below is a simple example that shows how to read JSON input to a POJO, and then convert that POJO to a line of CSV.

Here's our simple `Person` class:
```java
import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// annotation tells Jackson to ignore JSON fields that are not defined in this class. 
@JsonIgnoreProperties(ignoreUnknown = true) 
public class Person implements Serializable {

    private String firstName;
    private String lastName;
    private int age;

    public Person() {}

// .... getters and setters here .... //

}
```

Here is a transform that operates on a `Person` input and returns an output `String`
```java
 public class PersonToCsvRow extends PTransform<PCollection<Person>, PCollection<String>> {

     public static class ExtractRow extends SimpleFunction<Person, String> {

         @Override
         public String apply(Person input) {
             return input.getFirstName()
                 + "," + input.getLastName()
                 + "," + input.getAge();
         }
     }

     @Override
     public PCollection<String> expand(PCollection<Person> input) {
         return input.apply(MapElements.via(new ExtractRow()));
     }
 }
```

And finally, we create a simple pipeline with our custom transform:

```java
public class JsonToCsvPipeline {

    public static void main(String... args) throws IOException {

        PipelineOptionsFactory.register(InOutOptions.class); 
        InOutOptions options = PipelineOptionsFactory.as(InOutOptions.class);

        options.setJobName("Parse Json to CSV");
        options.setRunner(DirectRunner.class); 

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("Read source JSON file.", TextIO.read().from(options.getInput())) // Apache beam native transform to read/write from file
            .apply("Parse to POJO matching schema", ParseJsons.of(Person.class))     // Jackson transform to read json into an object. 
            .setCoder(SerializableCoder.of(Person.class))                            // Tells the pipeline how to serialize a `Person` object. 
            .apply("Create comma delimited string", new PersonToCsvRow())            // Our custom Transform. 
            .apply("Write out to file", TextIO.write().to(options.getOutput())       // Write out to CSV. 
                .withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
```

- The InOutOptions is a simple class Options class extension that tells the pipeline where to read and write from. It's implementation is found in this repo. 
- The DirectRunner class is apache Beam's local pipeline runner. It can be changed to use Apache Spark, Kafka, etc. 
