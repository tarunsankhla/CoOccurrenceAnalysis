/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.cooccurrenceanalysis;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class CoOccurrenceMapper extends Mapper<Object, Text, Text, Text> {
    private static final Logger LOG = Logger.getLogger(CoOccurrenceMapper.class);
    private Text movieKey = new Text();
    private Text stripe = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("Mapper setup started.");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Increment the input records counter
        context.getCounter("Mapper", "Input Records").increment(1);

        String[] parts = value.toString().split(",");
        if (parts.length == 4) {
            String userId = parts[0];
            String movieId = parts[1];

            HashSet<String> movieSet = new HashSet<>();
            movieSet.add(movieId);

            for (String movieA : movieSet) {
                HashMap<String, Integer> stripeMap = new HashMap<>();
                for (String movieB : movieSet) {
                    if (!movieA.equals(movieB)) {
                        stripeMap.put(movieB, stripeMap.getOrDefault(movieB, 0) + 1);
                    }
                }
                // Emit movieA as the key and its stripe as the value
                movieKey.set(movieA);
                stripe.set(stripeMap.toString());
                context.write(movieKey, stripe);

                // Log the emitted key-value pair
                LOG.debug("Mapper Emit: Key=" + movieKey + ", Value=" + stripe);
            }
        } else {
            // Log malformed records
            LOG.warn("Skipping malformed record: " + value.toString());
            context.getCounter("Mapper", "Malformed Records").increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Mapper cleanup completed.");
    }
}
