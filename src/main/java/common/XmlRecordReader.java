package common;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class XmlRecordReader extends RecordReader
{
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;
    private long startPosition;
    private long endPosition;
    private FSDataInputStream xmlInputStream;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        final FileSplit filesplit = (FileSplit) split;
        startPosition = filesplit.getStart();
        endPosition = startPosition + filesplit.getLength();
        final FileSystem fs = filesplit.getPath().getFileSystem(context.getConfiguration());
        xmlInputStream = fs.open(filesplit.getPath());
        xmlInputStream.seek(startPosition);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        final long total = endPosition - startPosition;
        final long read = xmlInputStream.getPos() - startPosition;
        return (float) read / total;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (xmlInputStream.getPos() >= endPosition)
        {
            return false;
        }

        byte[] startTag = "<page>".getBytes("utf-8");
        byte[] endTag = "</page>".getBytes("utf-8");

        if (readUntilMatch(startTag, false))
        {
            try
            {
                if (readUntilMatch(endTag, true))
                {
                    currentKey = new LongWritable(xmlInputStream.getPos());
                    currentValue = new Text();
                    currentValue.set(buffer.getData(), 0, buffer.getLength() - endTag.length);
                    return true;
                }
            }
            finally
            {
                buffer.reset();
            }
        }
        return false;
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException
    {
        int matchingByteIndex = 0;

        while (true)
        {
            int currentByte = xmlInputStream.read();
            if (currentByte == -1)
            {
                return false;
            }

            if (withinBlock)
            {
                buffer.write(currentByte);
            }

            if (currentByte == match[matchingByteIndex])
            {
                matchingByteIndex++;
                if (matchingByteIndex >= match.length)
                {
                    return true;
                }
            }
            else
            {
                matchingByteIndex = 0;
            }

            if (!withinBlock && matchingByteIndex == 0 && xmlInputStream.getPos() >= endPosition)
            {
                return false;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        xmlInputStream.close();
    }
}
