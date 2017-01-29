/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatedataanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ekta23
 */
public class MinMaxMapper extends Mapper<Object,Text,Text,MinMaxTuple>{
    
    private Text country=new Text();
    private Double minavgtemp;
    private Double maxavgtemp;
    
    private MinMaxTuple output = new MinMaxTuple();
    
    
    
    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        
         String[] input = value.toString().split(",");
         country.set(input[4]);
//         if ( minavgtemp == null || maxavgtemp== null) {
//			return;
//		}	
		try {
			output.setMinAvgTemp(Double.parseDouble(input[1]));
			output.setMaxAvgtemp(Double.parseDouble(input[1]));
			context.write(country,output);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
    
    
}
