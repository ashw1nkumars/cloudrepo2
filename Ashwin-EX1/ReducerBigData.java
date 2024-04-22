package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
public class ReducerBigData extends Reducer<
                Text,           // Input key type (word)
                IntWritable,    // Input value type (count of word)
                Text,           // Output key type (word)
                IntWritable> {  // Output value type (total count of word)

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // Sum up the counts for the same word
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);

        // Output word and its total count
        context.write(key, result);
    }
}
