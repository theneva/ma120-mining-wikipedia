package task_2.d;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UselessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable key, final Text article, final Context context) throws IOException, InterruptedException
    {
        if (article.toString().toLowerCase().contains("useless")) {
            context.write(new Text("articles_with_the_word_useless"), new IntWritable(1));
        }
    }
}
