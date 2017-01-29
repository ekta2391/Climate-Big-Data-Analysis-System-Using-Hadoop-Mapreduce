/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatepartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author ekta23
 */
public  class MyPartitioner extends Partitioner<Text, Text>{

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String[] str = value.toString().split(",");
         Double avgTemp = Double.parseDouble(str[1]);
         
         
             if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if( avgTemp<=5.0)
         {
            return 0;
         }
         else if(avgTemp>5.0 && avgTemp<=15.0)
         {
            return 1 % numReduceTasks;
         }
         else 
         {
            return 2 % numReduceTasks;
         }
         
         }
    
}
