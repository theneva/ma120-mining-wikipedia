package task_1.a;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String[] words = value.toString().split("\\s");

        for (final String word : words)
        {
            // Get rid of all commas, full stops, and anything after (including) apostrophes.
            final String modifiedWord = word
                    .replaceAll("[,.]", "")
                    .replaceAll("'.*", "");

            if (!modifiedWord.replaceAll("-", "").matches("^[a-zA-Z]+$"))
            {
                continue;
            }

            context.write(new Text(modifiedWord.toLowerCase()), new IntWritable(1));
        }
    }
}
