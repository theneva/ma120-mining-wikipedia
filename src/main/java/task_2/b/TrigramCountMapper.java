package task_2.b;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrigramCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable position, final Text articleAsText, final Context context) throws IOException, InterruptedException
    {
        final String article = articleAsText.toString();

        final String[] words = article.split("\\s");

        for (int i = 2; i < words.length; i++)
        {
            final String previousPreviousWord = words[i - 2].toLowerCase().replaceAll("[^a-z]", "");
            final String previousWord = words[i - 1].toLowerCase().replaceAll("[^a-z]", "");
            final String word = words[i].toLowerCase().replaceAll("[^a-z]", "");

            if (StringUtils.isBlank(previousPreviousWord) || StringUtils.isBlank(previousWord) || StringUtils.isBlank(word))
            {
                continue;
            }

            final String trigram = previousPreviousWord + ":" + previousWord + ":" + word;

            context.write(new Text(trigram), new IntWritable(1));
        }
    }
}
