bigrams_with_count = LOAD 'output-from-2a-mapreduce.txt' USING PigStorage('\t') AS (bigram:chararray, count:int);
bigrams_sorted = ORDER bigrams_with_count by count DESC;
most_popular_bigram = LIMIT bigrams_sorted 1;
DUMP most_popular_bigram;
