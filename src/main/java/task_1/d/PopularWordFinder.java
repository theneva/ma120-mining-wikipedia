package task_1.d;

import common.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task_1.b.WikiUniqueWordsCounter;

public class PopularWordFinder
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 3)
        {
            System.out.println("Usage: <class> <input path> <output path> <stopwords file>");
            return;
        }

        final String inputPath = args[0];
        final String outputPath = args[1];
        final String stopwordsPath = args[2];

        StopwordContainer.getInstance().initialize(stopwordsPath);

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WikiUniqueWordsCounter.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(PopularWordMapper.class);
        job.setReducerClass(PopularWordReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
