package common;

public class Util
{
    public static String stripForRegex(final String word)
    {
        return word
                .replaceAll("]", "")
                .replaceAll("\\[", "")
                .replaceAll("\\(", "")
                .replaceAll("\\)", "");
    }

    public static String cleanWikiGarbage(final String body)
    {
        return body
                .replaceAll("\n", " ")                      // newlines -> spaces
                .replaceAll("\\[\\[File:.*?]]", "")         // file links
                .replaceAll("\\[\\[Category:.*?]]", "")     // category links
                .replaceAll("\\[\\[Special:.*?]]", "")      // special links
                .replaceAll("\\[http://.*?]", "")           // external links
                .replaceAll("\\{\\{.*?}}", "")              // metadata enclosed in {{braces}}
                .replaceAll("\\s*(=+)[^=]*?(\\1)\\s*", "")  // === headings ===
                .replaceAll("&lt;ref.*?/ref&gt;", "")       // references
                .replaceAll("^\\s+", "")                    // leading spaces
                .replaceAll("wikt:", "")                    // wiktionary entity prefix
                .replaceAll("s:", "")                       // weird links
                .replaceAll("#.*?]]", "]]")                 // links to specific headers in entities
                .replaceAll("\\|.*?]]", "]]");              // differing display text in entity links
    }
}
