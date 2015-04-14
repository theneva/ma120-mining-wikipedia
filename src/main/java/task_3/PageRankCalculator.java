package task_3;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task_3.calculating.PageRankCalculateMapper;
import task_3.calculating.PageRankCalculateReducer;
import task_3.parsing.PageRankParseMapper;
import task_3.parsing.PageRankParseReducer;

import java.io.IOException;

public class PageRankCalculator
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 3)
        {
            System.out.println("Usage: <class> <input path> <parsing output path> <calculation output path>");
        }

        final String inputPath = args[0];
        final String parsingOutputPath = args[1];
        final String calculationOutputPath = args[2];

        runParseJob(inputPath, parsingOutputPath);
        runCalculationJob(parsingOutputPath, calculationOutputPath);
    }

    private static void runParseJob(final String inputPath, final String outputPath) throws IOException, InterruptedException, ClassNotFoundException
    {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PageRankCalculator.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapperClass(PageRankParseMapper.class);
        job.setReducerClass(PageRankParseReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    private static void runCalculationJob(final String inputPath, final String outputPath) throws IOException, InterruptedException, ClassNotFoundException
    {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PageRankCalculator.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(PageRankCalculateMapper.class);
        job.setReducerClass(PageRankCalculateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
