package sparkrdd.create;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import sparkrdd.create.CreatingRDD;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class CreatingRDDTest {

    private CreatingRDD creatingRDD;

    @Before
    public void setup() {
        creatingRDD = new CreatingRDD();
    }

    @Test
    public void testCreatingRDD() {
        List<String> list = Arrays.asList("English", "Hindi", "Marathi", "Maths", "Science", "Social Studies");
        JavaRDD<String> javaRDD = creatingRDD.createRDDUsingParallelize(list);
        Assert.assertArrayEquals(list.toArray(), javaRDD.collect().toArray());
    }
}
