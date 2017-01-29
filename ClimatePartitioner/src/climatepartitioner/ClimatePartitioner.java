/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatepartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ekta23
 */
public class ClimatePartitioner extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    

    @Override
    public int run(String[] arg) throws Exception {
        Configuration conf = getConf();
		
      Job job = Job.getInstance(conf, "topsal");
      job.setJarByClass(ClimatePartitioner.class);
		
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(ClimateMapper.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(MyPartitioner.class);
      job.setReducerClass(ClimateReducer.class);
      job.setNumReduceTasks(3);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);

     
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
    }
    public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new ClimatePartitioner(),args);
      System.exit(0);
    }
}
