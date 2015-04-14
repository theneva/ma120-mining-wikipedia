package task_1.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WikiOver2000WordsReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException
    {
        int numberOfArticles = 0;

        for (final IntWritable ignored : counts)
        {
            numberOfArticles++;
        }

        context.write(key, new IntWritable(numberOfArticles));
    }
}
