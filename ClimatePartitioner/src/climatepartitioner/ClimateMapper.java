/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatepartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ekta23
 */
public class ClimateMapper extends Mapper<LongWritable, Text, Text, Text>{
   
    public void map(LongWritable key, Text value, Context context)
      {
                 
         
          try{
              
            String [] str = value.toString().split(",");
           String country=str[4];
           Double temp = Double.parseDouble(str[1]);
           if(country!=null && temp !=null) {
               
               
                 context.write(new Text(country), new Text(value));
           }         
           
          
      }
	catch(Exception e)
         {
            System.out.println( "error" + e.getMessage());
         }
      }
    
}
