 package transforms;

 import pojos.Person;
 import org.apache.beam.sdk.transforms.MapElements;
 import org.apache.beam.sdk.transforms.PTransform;
 import org.apache.beam.sdk.transforms.SimpleFunction;
 import org.apache.beam.sdk.values.PCollection;


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
