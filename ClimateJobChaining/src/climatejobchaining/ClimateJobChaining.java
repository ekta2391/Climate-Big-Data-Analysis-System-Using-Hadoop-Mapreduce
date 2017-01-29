/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatejobchaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ekta23
 */
public class ClimateJobChaining extends Configured implements Tool {

    private static final String OUTPUT_PATH = "intermediate_output15";

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        if (args.length != 2) {
            System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
            System.exit(0);
        }
        ToolRunner.run(new Configuration(), new ClimateJobChaining(), args);

    }

    @Override
    public int run(String[] args) throws Exception {
        /**
         * Job1
         * **
         * **
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job 1");
        job.setJarByClass(ClimateJobChaining.class);
        job.setMapperClass(AverageMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        String output = args[1] + "intermediateF";
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);

        /*
         * Job 2
         */

        Job job2 = Job.getInstance(conf, "Job 2");
        job2.setJarByClass(ClimateJobChaining.class);
        job2.setMapperClass(MinMaxMapper.class);
        job2.setReducerClass(MinMaxReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(output+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        return job2.waitForCompletion(true) ? 0 : 1;
    }
    
}