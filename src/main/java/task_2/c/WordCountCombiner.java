package task_2.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
    {
        int count = 0;

        for (final IntWritable ignored : values)
        {
            count++;
        }

        context.write(key, new IntWritable(count));
    }
}
