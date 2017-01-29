/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatedataanalysis;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ekta23
 */
public class ClimateDataAnalysis {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Campaign Duration Count");
    job.setJarByClass(ClimateDataAnalysis.class);
    job.setMapperClass(MinMaxMapper.class);
    job.setReducerClass(MinMaxReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MinMaxTuple.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);   
    }
}
