trigrams_with_count = LOAD 'output-from-2b-mapreduce.txt' USING PigStorage('\t') AS (trigram:chararray, count:int);
trigrams_sorted = ORDER trigrams_with_count by count DESC;
most_popular_bigram = LIMIT trigrams_sorted 1;
DUMP most_popular_bigram;
