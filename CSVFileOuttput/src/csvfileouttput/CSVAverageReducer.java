/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package csvfileouttput;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ekta23
 */
public class CSVAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    private DoubleWritable result = new DoubleWritable(1);

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            int n = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                n++;
            }
            double avg = sum / n;
            result.set(avg);
            context.write(key, result);
        }
    
}
