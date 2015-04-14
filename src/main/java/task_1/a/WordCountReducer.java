package task_1.a;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable>
{
    @Override
    protected void reduce(final Text word, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        long sum = 0;

        for (final IntWritable count : counts)
        {
            // could just be sum++;
            sum += count.get();
        }

        System.out.println(word + ": " + sum);
        context.write(word, new LongWritable(sum));
    }
}
