package task_1.b;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UniqueWordCountReducer extends Reducer<Text, Void, Text, NullWritable>
{
    @Override
    protected void reduce(final Text key, final Iterable<Void> values, final Context context) throws IOException, InterruptedException
    {
        context.write(key, NullWritable.get());
    }
}
