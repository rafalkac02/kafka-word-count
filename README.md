# Word Count Per Record

Kafka application `WordCountPerLineApp` (using Kafka Producer and Consumer APIs) that does the following:

1. Consumes records from a topic, e.g. `input`
2. Counts words (in the value of a record)
3. Produces records with the unique words and their occurences (counts)
   - A record key -> hello hello world gives a record with the following value hello -> 2, world -> 1 (and the same key as in the input record)
4. (EXTRA) Produces as many records as there are unique words in the input record with their occurences (counts)
   - A record key -> hello hello world gives two records in the output, i.e. (hello, 2) and (world, 1 (as (key, value))

---
Credits: [kafka-workshop](https://jaceklaskowski.github.io/kafka-workshop/exercises/kafka-exercise-Word-Count-Per-Record.html)