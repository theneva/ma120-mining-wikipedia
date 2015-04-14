package task_2.b;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrigramCountFinder
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.out.println("Usage: <class> <input path>");
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TrigramCountFinder.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(TrigramCountMapper.class);
        job.setReducerClass(TrigramCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
