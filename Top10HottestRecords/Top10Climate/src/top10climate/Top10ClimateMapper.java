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
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ekta23
 */
public class Top10ClimateMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private TreeMap<Double, Text> recordMap = new TreeMap<Double, Text>();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Parse the input string into a nice map
        if (value.toString().contains("City")) {
            //do nothing
        } else {
            String[] input = value.toString().split(",");
            if (input.length > 6) {
                String country = input[4];
                if(input[1].length() > 0){
                Double avgTemp = Double.parseDouble(input[1]);
                recordMap.put(avgTemp, new Text(country));
                }
            }
        }
        if (recordMap.size() > 10) {
            recordMap.remove(recordMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Entry<Double, Text> entry : recordMap.entrySet()) {
            context.write(entry.getValue(), new DoubleWritable(entry.getKey()));
        }
    }

}
