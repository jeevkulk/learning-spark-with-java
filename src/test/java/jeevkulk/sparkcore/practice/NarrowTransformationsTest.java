package jeevkulk.sparkcore.practice;

import jeevkulk.sparkcore.practice.domain.Course;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class NarrowTransformationsTest {

    private Logger logger = LogManager.getLogger(NarrowTransformationsTest.class);

    NarrowTransformations narrowTransformations = NarrowTransformations.INSTANCE;
    private List<Course> courses1 = null;
    private List<Course> courses2 = null;

    @Before
    public void setup() {
        courses1 = Arrays.asList(
                new Course ("CE616", "Structural Dynamics",false,1,"CE4",6),
                new Course ("CE623", "Advanced Solid Mechanics",false,1,"CE4",6),
                new Course ("CE627", "Structural Design Lab",false,1,"CE4",4),
                new Course ("CE694", "Seminar", false,1,"CE4",4),
                new Course ("HS791", "Communication Skills",false,2,"CE4",2),
                new Course ("CE792", "Communication Skills",false,2,"CE4",4),
                new Course ("CE797", "I Stage Dissertation",false,3,"CE4",48),
                new Course ("CE798", "II Stage Dissertation",false,4,"CE4",42),
                new Course ("CE448", "Prestressed Concrete",true,0,"CE4",6),
                new Course ("CE482", "Construction Management",true,0,"CE4",6),
                new Course ("CE602", "Design of Offshore Structures",true,0,"CE4",6),
                new Course ("CE603", "Numerical Methods",true,0,"CE4",6),
                new Course ("CE605", "Applied Statistics",true,0,"CE4",6),
                new Course ("CE610", "Introduction to Earthquake Engineering",true,0,"CE4",6),
                new Course ("CE611", "Advanced Structural Mechanics",true,0,"CE4",6),
                new Course ("CE615", "Structural Optimisation",true,0,"CE4",6),
                new Course ("CE617", "Plates and Shells",true,0,"CE4",6),
                new Course ("CE619", "Structural Stability",true,0,"CE4",6),
                new Course ("CE620", "Finite Element Method",true,0,"CE4",6),
                new Course ("CE621", "Plastic Analysis",true,0,"CE4",6),
                new Course ("CE624", "Nonlinear Analysis",true,0,"CE4",6),
                new Course ("CE625", "Analysis of Offshore Structures",true,0,"CE4",6),
                new Course ("CE629", "Elastic Waves in Solids",true,0,"CE4",6),
                new Course ("CE633", "Soil Structure Interaction",true,0,"CE4",6),
                new Course ("CE639", "Green Building Design",true,0,"CE4",6),
                new Course ("CE640", "Foundation Engineering",true,0,"CE4",6),
                new Course ("CE647", "Soil Dynamics and Machine Foundations",true,0,"CE4",6),
                new Course ("CE651", "Bridge Engineering",true,0,"CE4",6),
                new Course ("CE653", "Structural Reliability and Risk Analysis",true,0,"CE4",6),
                new Course ("CE679", "Advanced Mechanics of Reinforced Concrete",true,0,"CE4",6),
                new Course ("CE684", "Advanced Geotechnical Earthquake Engineering",true,0,"CE4",6),
                new Course ("CE713", "Advanced Concrete Technology",true,0,"CE4",6),
                new Course ("CE719", "Construction Contracts",true,0,"CE4",6),
                new Course ("CE720", "Non-destructive Testing of Materials",true,0,"CE4",6),
                new Course ("CE727", "Construction Materials",true,0,"CE4",6),
                new Course ("CE743", "Condition Assessment and Rehabilitation of Structures",true,0,"CE4",6),
                new Course ("CE771", "Optimization in Civil Engineering",true,0,"CE4",6)
        );
        courses2 = Arrays.asList(
                new Course ("CE616", "Structural Dynamics",false,1,"CE4",6),
                new Course ("CE623", "Advanced Solid Mechanics",false,1,"CE4",6),
                new Course ("CE627", "Structural Design Lab",false,1,"CE4",4),
                new Course ("CE694_", "Seminar (Duplicated)", false,1,"CE4",4)
        );
    }

    @Test
    public void testFilterOutElectiveCourses() {
        JavaRDD<Course> rdd = narrowTransformations.filterOutElectiveCourses(courses1);
        Assert.assertTrue(rdd.count() == 8L);
    }

    @Test
    public void testGetAllCourseNamesUsingMap() {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaRDD<String> rdd = narrowTransformations.getAllCourseNamesUsingMap(courses1);
        sw.stop();
        logger.info("Time to get course names: "+sw.getTime(TimeUnit.MILLISECONDS));
        Assert.assertTrue(rdd.first() instanceof String);
    }

    @Test
    public void testGetAllCourseCodesUsingFlatMap() {
        JavaRDD<String> rdd = narrowTransformations.getAllCourseCodesUsingFlatMap(courses1);
        Assert.assertTrue(rdd.count() == 111L);
    }

    @Test
    public void testGetAllCourseNamesUsingMapPartition() {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaRDD<String> rdd = narrowTransformations.getAllCourseNamesUsingMapPartition(courses1);
        sw.stop();
        logger.info("Time to get course names: "+sw.getTime(TimeUnit.MILLISECONDS));
        Assert.assertTrue(rdd.first() instanceof String);
    }

    @Test
    public void testGetAllCourseNamesUsingMapPartitionsWithIndex() {
        StopWatch sw = new StopWatch();
        sw.start();
        JavaRDD<String> rdd = narrowTransformations.getAllCourseNamesUsingMapPartitionsWithIndex(courses1);
        sw.stop();
        logger.info("Time to get course names: "+sw.getTime(TimeUnit.MILLISECONDS));
        Assert.assertTrue(rdd.first() instanceof String);
    }

    @Test
    public void testGetSampleRDD() {
        JavaRDD<Course> rdd = narrowTransformations.getSampleRDD(courses1, false,0.1);
        Assert.assertTrue(rdd.count() == 4);
    }

    @Test
    public void testGetUnionRDD() {
        JavaRDD<Course> rdd = narrowTransformations.getUnionRDD(courses1, courses2);
        Assert.assertTrue(rdd.count() == 41);
    }
}
