package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
public class MapperBigData extends Mapper<
                    LongWritable,     // Input key type (offset in file)
                    Text,             // Input value type (line of text)
                    Text,             // Output key type (word)
                    IntWritable> {    // Output value type (count of word)

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into words based on whitespace
        String[] words = value.toString().split("\\s+");

        // Emit each word with a count of 1
        for (String w : words) {
            // Normalize word by converting to lowercase
            word.set(w.toLowerCase());
            context.write(word, one);
        }
    }
}
