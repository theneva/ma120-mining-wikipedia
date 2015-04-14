package task_3.calculating;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankCalculateMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final String record = value.toString();

        int titleTabIndex = record.indexOf("\t");
        int pageRankTabIndex = record.indexOf("\t", titleTabIndex + 1);

        final String title = record.substring(0, titleTabIndex);

        context.write(new Text(title), new Text("!"));

        // No links if this is -1.
        if (pageRankTabIndex == (-1)) {
            return;
        }

        final String titleAndRanking = value.toString().substring(0, pageRankTabIndex + 1);

        final String linksAsSingleString = record.substring(pageRankTabIndex + 1, record.length());
        final String[] links = linksAsSingleString.split(",");

        for (final String link : links)
        {
            final String totalLinks = titleAndRanking + links.length;
            context.write(new Text(link), new Text(totalLinks));
        }

        context.write(new Text(title), new Text("|" + linksAsSingleString));
    }
}
