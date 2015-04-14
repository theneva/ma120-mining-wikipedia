package task_1.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WikiOver2000WordsMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
    {
        final int articleLength = value.toString().split("\\s").length;

        if (articleLength > 2000)
        {
            context.write(new Text("above"), new IntWritable(1));
        }
        else
        {
            context.write(new Text("below"), new IntWritable(1));
        }
    }
}
