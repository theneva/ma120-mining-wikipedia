package task_1.d;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StopwordContainer
{
    private static StopwordContainer instance;
    private List<String> stopwords = new ArrayList<String>();

    private StopwordContainer()
    {
    }

    public static StopwordContainer getInstance()
    {
        return instance == null ? (instance = new StopwordContainer()) : instance;
    }

    public void initialize(final String filePath)
    {
        try
        {
            final BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));

            String word;

            while ((word = bufferedReader.readLine()) != null)
            {
                stopwords.add(word.toLowerCase());
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public List<String> getStopwords()
    {
        if (instance == null)
        {
            throw new RuntimeException("Initialize first!");
        }

        return stopwords;
    }
}
