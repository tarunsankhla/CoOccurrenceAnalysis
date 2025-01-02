
# Co-occurrence Analysis Using Stripes Pattern

This approach identifies which movies are often rated together by the same users. If a user rates **movieA** and **movieB**, a co-occurrence matrix is created to quantify the relationship. The **Stripes** pattern is used for efficiently computing pairwise associations (like co-occurrence matrices) by emitting a compact data structure (a **stripe**) that summarizes all pairs associated with a key, rather than emitting a separate key-value pair for each co-occurring pair.

---

## Problem

- **Input:** Ratings dataset (`ratings.csv`) with fields: `userId`, `movieId`, `rating`, `tstamp`.
- **Output:** A co-occurrence matrix showing how frequently movies are rated together by the same user.

---

## Mapper Phase

- For each user, gather all the movies they have rated.  
- For each movie **A** that a user has rated, create a **stripe** (e.g., a HashMap) recording co-occurrences with other movies **B** the user rated.
- A stripe might look like: `{ movieB1: count1, movieB2: count2, ... }`.

---

## Reducer Phase

- Receives stripes for a particular movie **A**.
- Merges these stripes to produce a final aggregated stripe for **A**.
- The result is a co-occurrence count between movie **A** and all other movies.

---

## Concurrency Considerations

- **Data Partitioning:** The data is partitioned by **movieA** so all stripes for a movie are sent to the same reducer.
- **Parallel Processing:** Multiple mappers run in parallel on different data splits; reducers handle different keys (movie IDs) in parallel.
- **Combining Stripes:** Since the stripes are associative and commutative, they can be merged in any order, enabling parallel merges without coordination.

---

## Commands

```bash
C:\Users\tarun\Documents\GitHub\INFO_7250_Big_data\final_project\CoOccurrenceAnalysis\target\CoOccurrenceAnalysis-1.0-SNAPSHOT.jar
scp /mnt/c/Users/tarun/Documents/GitHub/INFO_7250_Big_data/final_project/CoOccurrenceAnalysis/target/CoOccurrenceAnalysis-1.0-SNAPSHOT.jar hdoop@tarunsankhla:~/CoOccurrenceAnalysis-1.0-SNAPSHOT.jar
hadoop jar ~/CoOccurrenceAnalysis-1.0-SNAPSHOT.jar edu.neu.csye6220.cooccurrenceanalysis.CoOccurrenceAnalysis /final_project/ratings.csv /user/hdoop/cooccurrence_analysis
hdfs dfs -cat /user/hdoop/cooccurrence_analysis/part-r-00000 | head
hdfs dfs -cat /user/hdoop/cooccurrence_analysis/_SUCCESS | head
hdfs dfs -ls /user/hdoop/cooccurrence_analysis
hdfs dfs -rm -r /user/hdoop/cooccurrence_analysis
hadoop fs -mkdir /user/hdoop/movie_rating_sec_sort
```
![image](https://github.com/user-attachments/assets/4e4f2c8f-9438-4169-b8fd-78d2a06b0b78)
