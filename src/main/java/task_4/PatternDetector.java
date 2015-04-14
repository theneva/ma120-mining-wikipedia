package task_4;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PatternDetector
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 3)
        {
            System.out.println("Usage: <class> <input path> <bigram output path> <patterns output path>");
        }

        final String inputPath = args[0];
        final String bigramOutputPath = args[1];
        final String patternOutputPath = args[2];

        runBigramFindingJob(inputPath, bigramOutputPath);
        runBigramMatchingJob(bigramOutputPath, patternOutputPath);
    }

    private static void runBigramFindingJob(final String inputPath, final String outputPath) throws IOException, InterruptedException, ClassNotFoundException
    {
        final Job bigramFindingJob = Job.getInstance(new Configuration());
        bigramFindingJob.setJarByClass(PatternDetector.class);

        FileInputFormat.setInputPaths(bigramFindingJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(bigramFindingJob, new Path(outputPath));

        bigramFindingJob.setInputFormatClass(XmlInputFormat.class);

        bigramFindingJob.setMapOutputKeyClass(Text.class);
        bigramFindingJob.setMapOutputValueClass(Text.class);

        bigramFindingJob.setOutputKeyClass(Text.class);
        bigramFindingJob.setOutputValueClass(Text.class);

        bigramFindingJob.setMapperClass(PatternSentenceMapper.class);
        bigramFindingJob.setReducerClass(PatternBigramReducer.class);

        bigramFindingJob.waitForCompletion(true);
    }

    private static void runBigramMatchingJob(final String inputPath, final String outputPath) throws Exception
    {
        final Job bigramMatchingJob = Job.getInstance(new Configuration());
        bigramMatchingJob.setJarByClass(PatternDetector.class);

        FileInputFormat.setInputPaths(bigramMatchingJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(bigramMatchingJob, new Path(outputPath));

        bigramMatchingJob.setMapOutputKeyClass(Text.class);
        bigramMatchingJob.setMapOutputValueClass(IntWritable.class);

        bigramMatchingJob.setOutputKeyClass(Text.class);
        bigramMatchingJob.setOutputValueClass(IntWritable.class);

        bigramMatchingJob.setMapperClass(PatternBigramMatchingMapper.class);
        bigramMatchingJob.setReducerClass(PatternBigramMatchingReducer.class);

        bigramMatchingJob.waitForCompletion(true);
    }
}
