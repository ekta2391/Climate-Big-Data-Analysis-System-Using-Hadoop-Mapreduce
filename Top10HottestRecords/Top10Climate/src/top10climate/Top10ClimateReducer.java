/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package top10climate;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ekta23
 */
public class Top10ClimateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

    public void reduce(Text country, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            repToRecordMap.put(value.get(), country);
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        for (Entry<Double, Text> entry : repToRecordMap.descendingMap().entrySet()) {
            context.write(entry.getValue(), new DoubleWritable(entry.getKey()));
        }
    }

}
