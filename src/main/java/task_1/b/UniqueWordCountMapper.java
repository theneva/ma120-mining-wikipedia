package task_1.b;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UniqueWordCountMapper extends Mapper<LongWritable, Text, Text, NullWritable>
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

            System.out.println("k = " + modifiedWord.toLowerCase());
            context.write(new Text(modifiedWord.toLowerCase()), NullWritable.get());
        }
    }
}
