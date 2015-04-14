package task_1.b;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiUniqueWordsCounter
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.out.println("Usage: <class> <input path> <output path>");
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WikiUniqueWordsCounter.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(UniqueWordCountMapper.class);
        job.setReducerClass(UniqueWordCountReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);
    }
}
