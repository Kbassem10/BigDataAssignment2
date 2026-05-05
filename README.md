# CSCI 461: Introduction to Big Data - Assignment #2

**Team Members:**
* Karim Bassem Joseph (231000797)
* Ali Essam Ali (231001040)
* Omar Emad Sholkamy (231000802)

## Project Overview
This project contains our submission for Assignment #2, divided into two main parts:
1. **Part One:** MongoDB database deployment, data ingestion, and querying using a containerized environment.
2. **Part Two:** A Java-based Hadoop MapReduce job to calculate rating statistics from the converted dataset.

---

## Prerequisites: Installing Docker
To run this project, you must have Docker installed on your machine. 

* **Windows & Mac:** Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
* **Linux (Ubuntu/Debian):** Run the following commands in your terminal:
```bash
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```
Verify your installation by running `docker --version` in your terminal.

---

## Part One: MongoDB Setup & Execution

### 1. Start the MongoDB Container
We are using the official `mongo:7.0` image to ensure compatibility across all system architectures (including ARM/Apple Silicon). 

Run the following command to spin up the container in the background and map it to the default port:
```bash
docker run --name mongo -p 27017:27017 -d mongo:7.0
```
Verify the container is running by typing `docker ps`.

### 2. Import the Dataset
Ensure your terminal is in the same directory as the `mds.json` file. Copy the file into the running container:
```bash
docker cp mds.json mongo:/mds.json
```
Next, execute the `mongoimport` tool to create the `moviesDB` database, the `moviesColl` collection, and populate the data:
```bash
docker exec mongo mongoimport --db moviesDB --collection moviesColl --file /mds.json --jsonArray
```

### 3. Running the Queries
All required queries for this assignment (Indexing, Logical Operations, Aggregation, Querying, and the Bonus Distinct query) are documented in the `mDBQ.txt` file. 

To execute them, enter the MongoDB shell:
```bash
docker exec -it mongo mongosh
```
Switch to the correct database:
```javascript
use moviesDB
```
You can now copy and paste the queries from `mDBQ.txt` directly into the shell. The expected outputs are documented in `mDBR.txt` and the accompanying PDF report.

---

## Part Two: Hadoop MapReduce Execution

### 1. Data Preparation
The original `mds.json` file has been converted to `mds.csv`. Nested objects were flattened, array values were comma-separated within a single cell, and a header row was included.

### 2. Start the Hadoop Container
Launch the Hadoop container provided for the lab environments:
```bash
docker run -it --name hadoop-env -p 9870:9870 -p 8088:8088 <your-hadoop-image-name>
```

### 3. Load Data into HDFS
Copy the converted CSV file into the Hadoop container, and then move it into the Hadoop Distributed File System (HDFS):
```bash
docker cp mds.csv hadoop-env:/mds.csv
docker exec -it hadoop-env bash
hdfs dfs -mkdir -p /input
hdfs dfs -put /mds.csv /input/
```

### 4. Compile and Run the MapReduce Job
The MapReduce logic is contained entirely within `ARDriver.java` (including the Driver, Mapper, Reducer, and Bonus Combiner code). 

Compile the Java file and package it into a JAR, then execute the job:
```bash
hadoop com.sun.tools.javac.Main ARDriver.java
jar cf ar.jar ARDriver*.class
hadoop jar ar.jar ARDriver /input/mds.csv /output
```

### 5. Retrieve Results
Once the MapReduce job completes, retrieve the output statistics from HDFS:
```bash
hdfs dfs -cat /output/part-r-00000
```
*(The final output file `part-r-00000` is also included in the root of this submission zip).*
