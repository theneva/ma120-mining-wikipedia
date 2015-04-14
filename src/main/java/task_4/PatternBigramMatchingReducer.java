package task_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PatternBigramMatchingReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
    {
        int sum = 0;

        for (final IntWritable ignored : values)
        {
            sum++;
        }

        if (sum > 1)
        {
            context.write(key, new IntWritable(sum));
        }
    }
}
