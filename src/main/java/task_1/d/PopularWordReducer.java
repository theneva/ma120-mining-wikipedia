package task_1.d;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PopularWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
    {
        int wordCount = 0;

        for (final IntWritable ignored : values)
        {
            wordCount++;
        }

        context.write(key, new IntWritable(wordCount));
    }
}
