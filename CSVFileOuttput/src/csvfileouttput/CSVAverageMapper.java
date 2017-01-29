/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package csvfileouttput;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ekta23
 */
public class CSVAverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    private static DoubleWritable temp = new DoubleWritable(1);
        private Text country = new Text();
	  public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            if (value.toString().contains("Country")) {
                //do nothing
            } else {
                String[] input = value.toString().split(",");
                try{
                country.set(input[4]);
                temp.set(Double.parseDouble(input[1]));
               
                }
                catch(NumberFormatException n){
                    
                }
                 context.write(country, temp);
            }
          }
}
