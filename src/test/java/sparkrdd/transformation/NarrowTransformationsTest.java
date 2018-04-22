package sparkrdd.transformation;

import domain.Course;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class NarrowTransformationsTest {

    NarrowTransformations narrowTransformations = NarrowTransformations.INSTANCE;
    private List<Course> courses = null;

    @Before
    public void setup() {
        courses = Arrays.asList(
                new Course ("CE616", "Structural Dynamics"),
                new Course ("CE623", "Advanced Solid Mechanics"),
                new Course ("CE627", "Structural Design Lab"),
                new Course ("CE694", "Seminar"),
                new Course ("HS791", "Communication Skills"),
                new Course ("CE792", "Communication Skills"),
                new Course ("CE797", "I Stage Dissertation"),
                new Course ("CE798", "II Stage Dissertation"),
                new Course ("CE448", "Prestressed Concrete"),
                new Course ("CE482", "Construction Management"),
                new Course ("CE602", "Design of Offshore Structures"),
                new Course ("CE603", "Numerical Methods"),
                new Course ("CE605", "Applied Statistics"),
                new Course ("CE610", "Introduction to Earthquake Engineering"),
                new Course ("CE611", "Advanced Structural Mechanics"),
                new Course ("CE615", "Structural Optimisation"),
                new Course ("CE617", "Plates and Shells"),
                new Course ("CE619", "Structural Stability"),
                new Course ("CE620", "Finite Element Method"),
                new Course ("CE621", "Plastic Analysis"),
                new Course ("CE624", "Nonlinear Analysis"),
                new Course ("CE625", "Analysis of Offshore Structures"),
                new Course ("CE629", "Elastic Waves in Solids"),
                new Course ("CE633", "Soil Structure Interaction"),
                new Course ("CE639", "Green Building Design"),
                new Course ("CE640", "Foundation Engineering"),
                new Course ("CE647", "Soil Dynamics and Machine Foundations"),
                new Course ("CE651", "Bridge Engineering"),
                new Course ("CE653", "Structural Reliability and Risk Analysis"),
                new Course ("CE679", "Advanced Mechanics of Reinforced Concrete"),
                new Course ("CE684", "Advanced Geotechnical Earthquake Engineering"),
                new Course ("CE713", "Advanced Concrete Technology"),
                new Course ("CE719", "Construction Contracts"),
                new Course ("CE720", "Non-destructive Testing of Materials"),
                new Course ("CE727", "Construction Materials"),
                new Course ("CE743", "Condition Assessment and Rehabilitation of Structures"),
                new Course ("CE771", "Optimization in Civil Engineering")
        );
    }

    @Test
    public void testMapToCourseName() {
        JavaRDD<String> rdd = narrowTransformations.mapToCourseName(courses);
        Assert.assertTrue(rdd.first() instanceof String);
    }
}
