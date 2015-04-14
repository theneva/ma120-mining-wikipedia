package task_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PatternBigramMatchingMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(final LongWritable position, final Text input, final Context context) throws IOException, InterruptedException
    {
        context.write(input, new IntWritable(1));
    }
}
