package task_1.f;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LinkCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text link, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        int linkCount = 0;

        for (final IntWritable count : counts)
        {
            linkCount += count.get();
        }

        context.write(link, new IntWritable(linkCount));
    }
}
