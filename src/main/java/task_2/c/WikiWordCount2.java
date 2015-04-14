package task_2.c;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiWordCount2
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
        job.setJarByClass(WikiWordCount2.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(WordCountMapper2.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountReducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);
    }
}
