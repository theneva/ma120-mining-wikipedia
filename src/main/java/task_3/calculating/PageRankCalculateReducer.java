package task_3.calculating;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankCalculateReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(final Text title, final Iterable<Text> titleAndRankAsText, final Context context) throws IOException, InterruptedException
    {
        boolean articleIsKnown = false;

        float pagerank = 0;
        int inlinkCount = 0;
        int outlinkCount = 0;

        for (final Text value : titleAndRankAsText)
        {
            final String titleAndRankAndLinkCount = value.toString();

            if (titleAndRankAndLinkCount.equals("!"))
            {
                articleIsKnown = true;
                continue;
            }

            if (titleAndRankAndLinkCount.startsWith("|"))
            {
                continue;
            }

            final String[] titleAndRankAndLinkCountArray = titleAndRankAndLinkCount.split("\\t");

            final float individualPagerank = Float.parseFloat(titleAndRankAndLinkCountArray[1]);
            outlinkCount = Integer.parseInt(titleAndRankAndLinkCountArray[2]);

            pagerank += individualPagerank / outlinkCount;
            inlinkCount++;
        }

        if (!articleIsKnown) {
            return;
        }

        context.write(title, new Text(String.format("%s\t%s\t%s", pagerank, inlinkCount, outlinkCount)));
    }
}
