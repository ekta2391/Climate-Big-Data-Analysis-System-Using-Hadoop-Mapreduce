/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatepartitioner;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ekta23
 */
public class ClimateReducer extends Reducer<Text,Text,Text,Text>{
    
    public int max = -1;
      public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException
      {
       
                 
		
         context.write(new Text(key), new Text(value));
          
          
      }
}
