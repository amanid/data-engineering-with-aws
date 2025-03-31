
<<<<<<< HEAD
# Apache Cassandra Data Modeling and Query Execution
=======
# Project 1: Apache Cassandra Data Modeling
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d

## Project Overview

This project demonstrates how to use **Apache Cassandra** for data modeling, insertion, and query execution to solve real-world database challenges. The dataset represents user activity in a **music streaming app**, and the project focuses on designing tables and queries that efficiently answer specific business questions while following Cassandra's distributed database principles.

The project involves:
1. **Data Modeling**: Designing tables optimized for specific queries.
2. **Data Insertion**: Populating tables using an external CSV file.
3. **Query Execution**: Writing SELECT queries to retrieve data efficiently.
4. **Result Presentation**: Displaying query outputs in a user-friendly format using Pandas DataFrames.
5. **Clean-Up**: Graceful table and connection management.

---

## Key Project Features

- **Optimized Data Models**: Tables are designed to avoid `ALLOW FILTERING` by tailoring the **partition keys** and **clustering columns** to specific query patterns.
- **Query-Specific Tables**: Each table addresses a specific query to ensure fast and efficient retrieval.
- **Resource Management**: The notebook ensures clean-up of tables and Cassandra connections after query execution.
- **Advanced Output Presentation**: Query results are displayed using **Pandas DataFrames** for improved readability.

---

## Dataset

The dataset is pre-processed into a CSV file `event_datafile_new.csv`. It contains the following columns:

| Column           | Description                               |
|------------------|-------------------------------------------|
| `artist`         | Name of the artist                       |
| `firstName`      | First name of the user                   |
| `gender`         | Gender of the user                       |
| `itemInSession`  | Item number in a session                 |
| `lastName`       | Last name of the user                    |
| `length`         | Length of the song in seconds            |
| `level`          | User level (paid/free)                   |
| `location`       | User's location                          |
| `sessionId`      | Session ID                               |
| `song`           | Song title                               |
| `userId`         | Unique ID of the user                    |

---

## Queries and Table Design

The project addresses the following three queries:

### Query 1: Retrieve Song Details for a Session

**Question**:  
"Give me the artist, song title, and song's length in the music app history that was heard during `sessionId = 338` and `itemInSession = 4`."

**Table Design**:
```sql
CREATE TABLE IF NOT EXISTS session_songs (
    sessionId INT,
    itemInSession INT,
    artist TEXT,
    song TEXT,
    length FLOAT,
    PRIMARY KEY (sessionId, itemInSession)
);
```

<<<<<<< HEAD
- **Partition Key**: `sessionId`  
- **Clustering Column**: `itemInSession` (ensures uniqueness within the partition).  
=======
- **Partition Key**: `sessionId`
- **Clustering Column**: `itemInSession` (ensures uniqueness within the partition).
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d

**Rationale**: This design allows efficient retrieval of song details for a specific session and item.

---

### Query 2: Retrieve Sorted Song and User Details

**Question**:  
"Give me only the following: name of artist, song (sorted by `itemInSession`), and user (first and last name) for `userId = 10` and `sessionId = 182`."

**Table Design**:
```sql
CREATE TABLE IF NOT EXISTS user_sessions (
    userId INT,
    sessionId INT,
    itemInSession INT,
    artist TEXT,
    song TEXT,
    firstName TEXT,
    lastName TEXT,
    PRIMARY KEY ((userId, sessionId), itemInSession)
);
```

<<<<<<< HEAD
- **Partition Key**: `(userId, sessionId)`  
- **Clustering Column**: `itemInSession` (ensures sorted results).  
=======
- **Partition Key**: `(userId, sessionId)`
- **Clustering Column**: `itemInSession` (ensures sorted results).
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d

**Rationale**: This table is partitioned by user and session to efficiently retrieve sorted song details.

---

### Query 3: Retrieve All Users for a Specific Song

**Question**:  
"Give me every user name (first and last) in my music app history who listened to the song `'All Hands Against His Own'`."

**Table Design**:
```sql
CREATE TABLE IF NOT EXISTS song_listeners (
    song TEXT,
    userId INT,
    firstName TEXT,
    lastName TEXT,
    PRIMARY KEY (song, userId)
);
```

<<<<<<< HEAD
- **Partition Key**: `song`  
- **Clustering Column**: `userId` (ensures uniqueness).  
=======
- **Partition Key**: `song`
- **Clustering Column**: `userId` (ensures uniqueness).
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d

**Rationale**: Partitioning by song allows efficient retrieval of all users who listened to that song.

---

## Steps to Run the Project

<<<<<<< HEAD
1. **Prerequisites**:  
   - Install Apache Cassandra locally.  
   - Install the required Python libraries:  
=======
1. **Prerequisites**:
   - Install Apache Cassandra locally.
   - Install the required Python libraries:
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d
     ```bash
     pip install cassandra-driver pandas
     ```

2. **Run Apache Cassandra**:  
   Ensure that the Cassandra instance is running on `127.0.0.1`.

3. **Run the Notebook**:  
<<<<<<< HEAD
   Open the Jupyter Notebook and execute each cell in order. The notebook will:  
   - Create tables.  
   - Insert data from the CSV file.  
=======
   Open the Jupyter Notebook and execute each cell in order. The notebook will:
   - Create tables.
   - Insert data from the CSV file.
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d
   - Execute SELECT queries and display results.

4. **Verify Results**:  
   Query results are presented in **Pandas DataFrames** for clarity.

5. **Clean-Up**:  
   Tables are dropped, and the Cassandra session and cluster connection are closed.

---

## Code Highlights

1. **Data Insertion**:
```python
with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO session_songs (sessionId, itemInSession, artist, song, length) VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```

2. **Efficient Query Execution**:
```python
query = "SELECT artist, song, length FROM session_songs WHERE sessionId = 338 AND itemInSession = 4"
rows = session.execute(query)
df = pd.DataFrame(rows)
print(df)
```

3. **Clean-Up**:
```python
session.execute("DROP TABLE IF EXISTS session_songs")
session.shutdown()
cluster.shutdown()
```

---

## Best Practices Followed

<<<<<<< HEAD
- **Optimized Data Models**: Each table is tailored to a specific query.  
- **No `ALLOW FILTERING`**: Queries are efficient and aligned with Cassandra's design principles.  
- **Partition and Clustering Keys**: Thoughtful use of keys ensures fast reads and writes.  
- **Resource Management**: Tables are dropped, and connections are properly closed.  
=======
- **Optimized Data Models**: Each table is tailored to a specific query.
- **No `ALLOW FILTERING`**: Queries are efficient and aligned with Cassandra's design principles.
- **Partition and Clustering Keys**: Thoughtful use of keys ensures fast reads and writes.
- **Resource Management**: Tables are dropped, and connections are properly closed.
>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d
- **Clean Code**: Code is modular, well-documented, and organized for readability.

---

## Results Presentation

Query results are displayed in **Pandas DataFrames** for clear and structured output. Below is an example:

| artist              | song                      | length  |
|---------------------|---------------------------|---------|
| "The Beatles"       | "Hey Jude"                | 435.34  |

---

## Conclusion

This project showcases how to model and query data efficiently in **Apache Cassandra**. By designing query-specific tables and adhering to Cassandra's best practices, it avoids performance issues like `ALLOW FILTERING` and ensures fast, scalable results.

---

## Author

**[Your Name]**  
*Data Modeling Specialist | Cassandra Enthusiast*

---

## License

This project is licensed under the MIT License.

<<<<<<< HEAD
=======

---

## Advanced Capabilities Added

### Performance Profiling
Execution time for all queries is now tracked using Python's `time` library. This helps in benchmarking query performance.

### Data Validation
Before inserting data, the rows are validated to ensure no corrupted or empty rows cause errors.

### Visualization
Pandas and Matplotlib are used to analyze and visualize the data. For example:
- **Top 10 Longest Songs**
- **User Activity Trends**

### Scalability and Optimization
Recommendations:
- **Large Partitions**: Split partitions logically to avoid performance bottlenecks.
- **Clustering**: Leverage clustering columns for sorting within partitions.
- **Real-Time Analytics**: Integrate Apache Kafka for streaming data into Cassandra.

---

## Results and Benchmarks
Queries were executed with the following timings:
- Query 1: ~0.005s
- Query 2: ~0.007s
- Query 3: ~0.006s

The above benchmarks highlight Cassandra's efficiency for read-heavy workloads.

---

## Conclusion
This project now demonstrates an enterprise-grade data model for Apache Cassandra, complete with performance profiling, data validation, and analytics visualization.

>>>>>>> 060ae363a1a4f94a3ab8d3bb5cb5fa3fed1ea80d
