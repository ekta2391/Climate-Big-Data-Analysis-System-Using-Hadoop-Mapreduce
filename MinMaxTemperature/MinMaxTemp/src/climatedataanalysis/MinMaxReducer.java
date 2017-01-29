/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatedataanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ekta23
 */
public class MinMaxReducer extends Reducer<Text,MinMaxTuple,Text,MinMaxTuple>{
    private MinMaxTuple resultRow = new MinMaxTuple();
	 
     public void reduce(Text key, Iterable<MinMaxTuple> values,Context context) throws IOException, InterruptedException {
      Double minavgtemp = 0.0;
      Double maxavgtemp = 0.0;
      
      resultRow.setMinAvgTemp(0.0);
      resultRow.setMaxAvgtemp(0.0);
      
      for(MinMaxTuple value:values){
          
          minavgtemp = value.getMinAvgTemp();
          maxavgtemp = value.getMaxAvgtemp();
          
          if(resultRow.getMinAvgTemp()==0.0 || minavgtemp.compareTo(resultRow.getMinAvgTemp())<0.0){
              resultRow.setMinAvgTemp(minavgtemp);
          }
          
          if(resultRow.getMaxAvgtemp()==0.0 || maxavgtemp.compareTo(resultRow.getMaxAvgtemp())>0.0){
              resultRow.setMaxAvgtemp(maxavgtemp);
          }
      }
    
              context.write(key, resultRow);

}
}
