package task_3.parsing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankParseReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(final Text article, final Iterable<Text> links, final Context context) throws IOException, InterruptedException
    {
        String initialPageRank = "0.25\t";

        boolean firstLink = true;

        for (final Text link : links)
        {
            if (!firstLink)
            {
                initialPageRank += ",";
            }

            initialPageRank += link.toString();
            firstLink = false;
        }

        context.write(article, new Text(initialPageRank));
    }
}
