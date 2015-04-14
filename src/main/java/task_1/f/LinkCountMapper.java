package task_1.f;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.Util.cleanWikiGarbage;

public class LinkCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String fullArticle = value.toString();

        final String cleanedArticle = cleanWikiGarbage(fullArticle);

        final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)]]");
        final Matcher linkMatcher = linkPattern.matcher(cleanedArticle);

        while (linkMatcher.find())
        {
            context.write(new Text(linkMatcher.group(1)), new IntWritable(1));
        }
    }
}
