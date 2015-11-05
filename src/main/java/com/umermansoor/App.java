package com.umermansoor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * The main application class. 
 * 
 * @author Umer Mansoor
 */
public class App extends Configured implements Tool
{
    /**
     * Application entry point.
     * @param args
     * @throws Exception - Bad idea but produces less cluttered code.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output mongourl>");
            return 1;
        }
        // Create the job specification object
        Configuration conf = getConf();
        
        MongoConfigUtil.setOutputURI(conf, args[1]);
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(App.class);
        job.setJobName("Earthquake Measurement");

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(MongoOutputFormat.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(EarthquakeMapper.class);
        job.setReducerClass(EarthquakeReducer.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(DoubleWritable.class);
        job.setOutputValueClass(BSONWritable.class);

        // Wait for the job to finish before terminating
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new App(), args);
		System.exit(res);
	}
}
