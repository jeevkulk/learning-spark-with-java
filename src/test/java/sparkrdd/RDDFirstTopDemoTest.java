package sparkrdd;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class RDDFirstTopDemoTest {

    private RDDFirstTopDemo rddFirstTopDemo = null;

    @Before
    public void setup() {
        rddFirstTopDemo = RDDFirstTopDemo.INSTANCE;
    }

    @Test
    public void testRDDFirst() {
        List<String> coloursList = Arrays.asList("Red","Orange","Yellow","Green","Blue","Indigo","Violet");
        String firstStr = rddFirstTopDemo.rddFirstDemo(coloursList);
        Mockito.times(1);
    }

    @Test
    public void testTopDemoOnStringRDD() {
        List<String> coloursList = Arrays.asList("Red","Orange","Yellow","Green","Blue","Indigo","Violet");
        rddFirstTopDemo.rddTopDemoOnStringRDD(coloursList);
        Mockito.times(1);
    }

    @Test
    public void testTopDemoOnObjectRDD() {
        List<RDDFirstTopDemo.Course> coursesList = Arrays.asList(
                new RDDFirstTopDemo.Course("English"),
                new RDDFirstTopDemo.Course("Hindi"),
                new RDDFirstTopDemo.Course("Marathi"),
                new RDDFirstTopDemo.Course("Maths"),
                new RDDFirstTopDemo.Course("Science"),
                new RDDFirstTopDemo.Course("Social Studies")
        );
        rddFirstTopDemo.rddTopDemoOnCustomObjectRDD(coursesList);
        Mockito.times(1);
    }
}
