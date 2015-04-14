package task_4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static common.Util.stripForRegex;

public class PatternBigramReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(final Text entityAsText, final Iterable<Text> sentences, final Context context) throws IOException, InterruptedException
    {
        final String entity = entityAsText.toString();

        for (final Text sentenceAsText : sentences)
        {
            // todo we don't consider multiple mentions of the entity within a single sentence.
            final String sentence = sentenceAsText.toString();

            final String nextEntity = findNextEntity(sentence, entity);

            if (nextEntity == null)
            {
                continue;
            }

            final String[] keyComponents = new String[]{entity, nextEntity};
            Arrays.sort(keyComponents);

            final String key = keyComponents[0] + ":" + keyComponents[1];

            final List<String> wordsBetweenEntities = findWordsBetweenEntities(sentence, entity, nextEntity);
            final List<Bigram> bigrams = findBigrams(wordsBetweenEntities);

            if (bigrams == null)
            {
                continue;
            }

            for (final Bigram bigram : bigrams)
            {
                if (StringUtils.isEmpty(bigram.getFirst()))
                {
                    continue;
                }

                if (StringUtils.isEmpty(bigram.getSecond()))
                {
                    continue;
                }

                final String value = bigram.getFirst() + ":" + bigram.getSecond();

                context.write(new Text(key), new Text(value));
            }
        }
    }

    // TODO just split and iterate instead of this?
    private String findNextEntity(final String sentence, final String entity)
    {
        final Scanner scanner = new Scanner(sentence);

        scanner.findInLine("\\[\\[" + stripForRegex(entity) + "]]");

        while (scanner.hasNext())
        {
            final String token = scanner.next();

            if (token.matches("\\[\\[(.*?)]]"))
            {
                return token
                        .toLowerCase()
                        .replaceAll("[^a-z]", "");
            }
        }

        return null;
    }

    // TODO just split and iterate instead of this?
    private List<String> findWordsBetweenEntities(final String sentence, final String first, final String second)
    {
        final Scanner scanner = new Scanner(sentence);

        scanner.findInLine("\\[\\[" + stripForRegex(first) + "]]");

        final List<String> words = new ArrayList<>();

        while (scanner.hasNext())
        {
            final String token = scanner.next();

            if (token.equals("[[" + second + "]]"))
            {
                break;
            }

            words.add(token);
        }

        return words;
    }

    private List<Bigram> findBigrams(final List<String> words)
    {
        if (words.size() < 2)
        {
            // todo returning null is bad practise.
            return null;
        }

        final List<Bigram> bigrams = new ArrayList<>();

        for (int i = 0; i < words.size() - 1; i++)
        {
            bigrams.add(new Bigram(stripForRegex(words.get(i)), stripForRegex(words.get(i + 1))));
        }

        return bigrams;
    }
}
