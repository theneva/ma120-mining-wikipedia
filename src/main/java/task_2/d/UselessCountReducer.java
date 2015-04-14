package task_2.d;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UselessCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        int uselessCount = 0;

        for (final IntWritable count : counts)
        {
            uselessCount += count.get();
        }

        context.write(key, new IntWritable(uselessCount));
    }
}
