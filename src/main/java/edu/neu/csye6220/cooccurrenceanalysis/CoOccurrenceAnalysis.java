/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package edu.neu.csye6220.cooccurrenceanalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class CoOccurrenceAnalysis {
    private static final Logger LOG = Logger.getLogger(CoOccurrenceAnalysis.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Measure start time
        long startTime = System.currentTimeMillis();

        Job job = Job.getInstance(conf, "Co-Occurrence Analysis");
        job.setJarByClass(CoOccurrenceAnalysis.class);
        job.setMapperClass(CoOccurrenceMapper.class);
        job.setReducerClass(CoOccurrenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the number of reducers (optional)
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        LOG.info("Job configuration completed. Starting the job...");

        boolean success = job.waitForCompletion(true);

        // Measure end time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        LOG.info("Job completed in " + totalTime + " milliseconds.");

        // Print counters
        CounterGroup mapperCounters = job.getCounters().getGroup("Mapper");
        CounterGroup reducerCounters = job.getCounters().getGroup("Reducer");

        LOG.info("Mapper Counters:");
        for (org.apache.hadoop.mapreduce.Counter counter : mapperCounters) {
            LOG.info(counter.getDisplayName() + ": " + counter.getValue());
        }

        LOG.info("Reducer Counters:");
        for (org.apache.hadoop.mapreduce.Counter counter : reducerCounters) {
            LOG.info(counter.getDisplayName() + ": " + counter.getValue());
        }

        System.exit(success ? 0 : 1);
    }
}
