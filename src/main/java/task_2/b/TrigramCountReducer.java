package task_2.b;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrigramCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text bigram, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        int occurrences = 0;

        for (final IntWritable count : counts)
        {
            occurrences += count.get();
        }

        context.write(bigram, new IntWritable(occurrences));
    }
}
