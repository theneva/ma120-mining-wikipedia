package task_2.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer2 extends Reducer<Text, IntWritable, Text, LongWritable>
{
    @Override
    protected void reduce(final Text word, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        long sum = 0;

        for (final IntWritable count : counts)
        {
            // The combiner has hopefully changed the count, so sum++ cannot be used here.
            sum += count.get();
        }

        context.write(word, new LongWritable(sum));
    }
}
