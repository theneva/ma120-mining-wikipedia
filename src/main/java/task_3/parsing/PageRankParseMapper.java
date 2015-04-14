package task_3.parsing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.Util.cleanWikiGarbage;

public class PageRankParseMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(final LongWritable key, final Text articleAsText, final Context context) throws IOException, InterruptedException
    {
        final String article = cleanWikiGarbage(articleAsText.toString());

        final Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
        final Matcher titleMatcher = titlePattern.matcher(article);

        if (!titleMatcher.find())
        {
            return;
        }

        final String title = titleMatcher.group(1).replaceAll("\\s", "_").toLowerCase();

        final Set<String> links = getLinks(article);

        for (final String link : links)
        {
            // No self-referencing allowed.
            if (link.equals(title)) {
                continue;
            }

            context.write(new Text(title), new Text(link));
        }
    }

    private Set<String> getLinks(final String article)
    {
        final Set<String> links = new HashSet<>();

        final Pattern linkPattern = Pattern.compile("\\[\\[(.+?)]]");
        final Matcher linkMatcher = linkPattern.matcher(article);

        while (linkMatcher.find())
        {
            links.add(linkMatcher.group(1).replaceAll("\\s", "_").toLowerCase());
        }

        return links;
    }
}
