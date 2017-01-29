/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatejobchaining;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ekta23
 */
public class MinMaxReducer extends Reducer<Text, Text, Text, NullWritable> {

    private MinMaxTuple result = new MinMaxTuple();
    String minCountry = "";
    String maxCountry = "";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        result.setMaxavgtemp(0);
        result.setMinavgtemp(0);

        for (Text value : values) {
            String[] text = value.toString().split("\t");
            if (text.length == 2) {
                String[] val = text[1].toString().split(" ");
                if (val.length > 1) {
                    if (result.getMaxavgtemp() == 0 && result.getMinavgtemp() == 0) {
                        result.setMaxavgtemp(Double.parseDouble(val[2]));
                        result.setMinavgtemp(Double.parseDouble(val[2]));
                        result.setMaxCountry(val[0]);
                        result.setMinCountry(val[0]);
                    } else {
                        if (result.getMaxavgtemp() <= Double.parseDouble(val[2])) {
                            result.setMaxavgtemp(Double.parseDouble(val[2]));
                            result.setMaxCountry(val[0]);

                        } else {
                            if (result.getMinavgtemp() >= Double.parseDouble(val[2])) {
                                result.setMinavgtemp(Double.parseDouble(val[2]));
                                result.setMinCountry(val[0]);
                            }
                        }
                    }
                    context.write(new Text(result.toString()), NullWritable.get());
                }
                
            }
        }

    }
}