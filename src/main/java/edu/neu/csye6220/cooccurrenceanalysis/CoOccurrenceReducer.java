/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.csye6220.cooccurrenceanalysis;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CoOccurrenceReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOG = Logger.getLogger(CoOccurrenceReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("Reducer setup started.");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Increment the input groups counter
        context.getCounter("Reducer", "Input Groups").increment(1);

        HashMap<String, Integer> aggregatedStripe = new HashMap<>();

        for (Text value : values) {
            // Parse the stripe from the mapper
            String stripeString = value.toString();
            stripeString = stripeString.substring(1, stripeString.length() - 1); // Remove curly braces
            String[] pairs = stripeString.split(", ");

            for (String pair : pairs) {
                if (!pair.isEmpty()) {
                    String[] keyValue = pair.split("=");
                    if (keyValue.length == 2) {
                        String movieB = keyValue[0];
                        int count = Integer.parseInt(keyValue[1]);
                        aggregatedStripe.put(movieB, aggregatedStripe.getOrDefault(movieB, 0) + count);
                    }
                }
            }
        }

        // Emit the aggregated stripe for this movie
        context.write(key, new Text(aggregatedStripe.toString()));

        // Log the output key-value pair
        LOG.debug("Reducer Emit: Key=" + key + ", Value=" + aggregatedStripe.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Reducer cleanup completed.");
    }
}
