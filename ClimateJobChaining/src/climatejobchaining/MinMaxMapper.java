/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatejobchaining;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ekta23
 */
public class MinMaxMapper extends Mapper<LongWritable, Text, Text, Text> {

    private MinMaxTuple outTuple = new MinMaxTuple();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().contains(" ")) {
            //do nothing
        } else {
            String[] input = value.toString().split("\t");
            outTuple.setMinCountry(input[0]);
            outTuple.setMaxCountry(input[0]);
            outTuple.setMinavgtemp(Double.parseDouble(input[1]));
            outTuple.setMaxavgtemp(Double.parseDouble(input[1]));
            context.write(new Text("abc"), new Text(outTuple.toString()));
        }
    }
}
