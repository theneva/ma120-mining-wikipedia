popular_words = LOAD 'output-from-1d.txt' USING PigStorage('\t') AS (popular_word:chararray);
links = LOAD 'output_from_1f.txt' USING PigStorage('\t') AS (link:chararray);
link_words = FOREACH links GENERATE FLATTEN(TOKENIZE(LOWER(link))) AS link_word;
popular_words_joined_with_links = JOIN popular_words BY popular_word, link_words BY link_word;
popular_words_in_links = FOREACH popular_words_joined_with_links GENERATE popular_word;
distinct_popular_words_in_links = DISTINCT popular_words_in_links;
DUMP distinct_popular_words_in_links;
