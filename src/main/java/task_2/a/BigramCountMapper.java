package task_2.a;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static common.Util.cleanWikiGarbage;

public class BigramCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable position, final Text articleAsText, final Context context) throws IOException, InterruptedException
    {
        final String article = cleanWikiGarbage(articleAsText.toString());

        final String[] words = article.split("\\s");

        for (int i = 1; i < words.length; i++)
        {
            final String previousWord = words[i - 1].toLowerCase().replaceAll("[^a-z]", "");
            final String word = words[i].toLowerCase().replaceAll("[^a-z]", "");


            if (StringUtils.isBlank(previousWord) || StringUtils.isBlank(word))
            {
                continue;
            }

            final String bigram = previousWord + ":" + word;

            context.write(new Text(bigram), new IntWritable(1));
        }
    }
}
