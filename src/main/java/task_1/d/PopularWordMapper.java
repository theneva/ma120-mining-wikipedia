package task_1.d;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopularWordMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] words = value.toString().split("\\s");

        for (final String word : words)
        {
            if (StopwordContainer.getInstance().getStopwords().contains(word.toLowerCase()))
            {
                continue;
            }

            final String modifiedWord = word
                    .replaceAll("[^a-z\\-]", "");

            // write if a valid word
            if (modifiedWord.toLowerCase().matches("[a-z]+\\-[a-z]+"))
            {
                context.write(new Text(modifiedWord), new IntWritable(1));
            }
        }
    }
}
