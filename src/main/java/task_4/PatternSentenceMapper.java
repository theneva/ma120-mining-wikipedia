package task_4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static common.Util.cleanWikiGarbage;
import static common.Util.stripForRegex;

public class PatternSentenceMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(final LongWritable key, final Text value, final Context context)
    {
        final String fullPage = value.toString();

        final List<String> probableSentences = findProbableSentences(fullPage);

        final List<EntitySentencePair> entitySentencePairs = findEntitySentencePairs(probableSentences);

        entitySentencePairs.forEach(entitySentencePair -> {
            try
            {
                context.write(
                        new Text(entitySentencePair.getEntity()),
                        new Text(entitySentencePair.getSentence())
                );
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
    }

    private List<String> findProbableSentences(final String fullPage)
    {
        final String body = getArticleBody(fullPage);
        final String fullPageWithoutHeaders = cleanWikiGarbage(body);

        // Full stop followed indicates possible sentence end.
        final String[] possibleSentences = fullPageWithoutHeaders.split("\\.(\\s)+");

        final List<String> probableSentences = new ArrayList<>();
        for (int sentenceIndex = 1; sentenceIndex < possibleSentences.length; sentenceIndex++)
        {
            final String possibleSentence = possibleSentences[sentenceIndex - 1];

            // Remove book references.
            if (possibleSentence.matches(".*ISBN [0-9].*"))
            {
                continue;
            }

            // Remove list entries.
            if (possibleSentence.matches(".*\\*.*"))
            {
                continue;
            }

            final String currentSentence = possibleSentences[sentenceIndex];

            // capital letter indicates that the previous segment was likely a sentence.
            // A sentence is almost always > 2 characters long.
            if (currentSentence.matches("^'*?[A-Z\\[].*") && currentSentence.length() > 2)
            {
                // The sentence needs at least two entities to be useful. This is not really a concern of this mapper, but
                // greatly increases performance and decreases output size.
                if (possibleSentence.matches(".*\\[\\[.*?]].*\\[\\[.*?]].*"))
                {
                    probableSentences.add(possibleSentence);
                }
            }
            else
            {
                possibleSentences[sentenceIndex] = possibleSentence + ". " + currentSentence;
            }
        }

        probableSentences.add(possibleSentences[possibleSentences.length - 1]);
        return probableSentences;
    }

    private String getArticleBody(final String fullPage)
    {
        final String textStartTag = "<text xml:space=\"preserve\">";
        final String textEndTag = "</text>";

        return fullPage.substring(
                fullPage.indexOf(textStartTag) + textStartTag.length(),
                fullPage.indexOf(textEndTag)
        );
    }

    private List<EntitySentencePair> findEntitySentencePairs(final List<String> sentences)
    {
        final List<EntitySentencePair> entities = new ArrayList<>();

        for (final String sentence : sentences)
        {
            final Pattern entityPattern = Pattern.compile("\\[\\[(.*?)]]");
            final Matcher entityMatcher = entityPattern.matcher(sentence);

            while (entityMatcher.find())
            {
                final String match = stripForRegex(entityMatcher.group(1));

                if (StringUtils.isBlank(match))
                {
                    continue;
                }

                // if the sentence contains more entities
                // todo this ignores entities with special characters stripped away earlier in this method.
                if (sentence.matches(".*\\[\\[" + match + "]].*\\[\\[.*?]].*"))
                {
                    entities.add(new EntitySentencePair(match, sentence));
                }
            }
        }

        return entities;
    }
}
