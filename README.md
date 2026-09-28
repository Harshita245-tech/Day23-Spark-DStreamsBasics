# 🚀 Day 23 - Apache Spark DStreams Basics

## 📌 Project Overview

Day 23 focuses on **Apache Spark DStreams (Discretized Streams)** and micro-batch stream processing using Scala.

The project demonstrates how Spark Streaming can receive real-time application logs through a TCP socket using `nc`, process the incoming data in micro-batches, and perform basic transformations such as `map`, `filter`, and `flatMap`.

The main scenario is **application log monitoring**, where `ERROR` messages are identified and counted for every micro-batch.

---

## 🎯 Objectives

- Create a Spark Streaming Context
- Define a batch interval
- Read real-time data from a TCP socket
- Use `nc` as the log producer
- Apply `map()` transformation
- Apply `filter()` transformation
- Apply `flatMap()` transformation
- Count `ERROR` messages
- Understand micro-batch processing
- Process application logs in real time

---

## 🛠️ Technologies Used

- Apache Spark 3.5.3
- Spark Streaming
- Scala 2.12.18
- SBT
- Ubuntu/Linux
- TCP Socket
- Netcat (`nc`)
- Git & GitHub

---

## 📁 Project Structure

```text
day23-spark/
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── Day23DStreamsBasics.scala
│
├── .gitignore
├── build.sbt
└── README.md
```

---

## 🔄 DStreams Processing Flow

```text
Application Logs
       ↓
     nc
       ↓
 TCP Socket : 9999
       ↓
 Spark DStream
       ↓
 ┌───────────────┐
 │     MAP       │ → Convert logs to uppercase
 └───────────────┘
       ↓
 ┌───────────────┐
 │    FILTER     │ → Select ERROR logs
 └───────────────┘
       ↓
 ┌───────────────┐
 │    FLATMAP     │ → Split logs into words
 └───────────────┘
       ↓
 ERROR Count
       ↓
 Micro-Batch Output
```

---

## ⏱️ Micro-Batch Processing

The application uses a **5-second batch interval**.

```text
Batch 1 → 0-5 seconds
Batch 2 → 5-10 seconds
Batch 3 → 10-15 seconds
Batch 4 → 15-20 seconds
...
```

Spark Streaming collects incoming records during each interval and processes them as a small batch.

This approach is called **micro-batch processing**.

---

## 🔌 Socket Stream

The application reads real-time log messages from:

```text
localhost:9999
```

The Spark application uses:

```scala
val lines = ssc.socketTextStream("localhost", 9999)
```

A separate terminal uses `nc` to send log messages to this socket.

---

## 🖥️ Running the Application

### Step 1 - Start Spark Application

Open the first terminal:

```bash
cd ~/day23-spark
sbt run
```

The application displays:

```text
Streaming application started
Batch interval : 5 seconds
Socket         : localhost:9999
Waiting for log messages...
Send messages using nc in another terminal.
```

---

### Step 2 - Start Netcat

Open a second terminal:

```bash
nc -lk 9999
```

The terminal waits for incoming log messages.

---

### Step 3 - Send Application Logs

Enter messages such as:

```text
INFO Application started
ERROR Database connection failed
INFO User login successful
ERROR Payment timeout
INFO Transaction completed
```

Press **Enter** after each message.

Spark receives these messages through the socket and processes them during the next micro-batch.

---

## 🔧 DStream Transformations

### 1. MAP

The `map()` transformation converts every log message to uppercase.

```scala
val upperCaseLogs = lines.map(_.toUpperCase)
```

Example:

```text
Input:
INFO Application started

Output:
INFO APPLICATION STARTED
```

---

### 2. FILTER

The `filter()` transformation selects only messages containing `ERROR`.

```scala
val errorLogs = lines.filter(_.contains("ERROR"))
```

Example:

```text
INFO Application started
ERROR Database connection failed
INFO User login successful
```

Output:

```text
ERROR Database connection failed
```

---

### 3. FLATMAP

The `flatMap()` transformation splits each log message into individual words.

```scala
val words = lines.flatMap(_.split("\\s+"))
```

Example:

```text
ERROR Database connection failed
```

Output:

```text
ERROR
Database
connection
failed
```

---

## 🚨 ERROR Message Counting

The project counts the number of `ERROR` messages received in every micro-batch.

```scala
val errorCount = errorLogs
  .map(_ => 1)
  .reduce(_ + _)
```

For example:

```text
ERROR Database connection failed
ERROR Payment timeout
```

Output:

```text
ERROR messages in this batch: 2
```

---

## 📊 Sample Output

The application successfully produced output such as:

```text
========== MAP ==========
INFO APPLICATION STARTED

========== FLATMAP - WORDS ==========
INFO
Application
started
```

For an error log:

```text
========== MAP ==========
ERROR DATABASE CONNECTION FAILED

========== FILTER - ERROR LOGS ==========
ERROR Database connection failed

========== FLATMAP - WORDS ==========
ERROR
Database
connection
failed

========== ERROR COUNT ==========
ERROR messages in this batch: 1
=================================
```

Another error message produced:

```text
========== MAP ==========
ERROR PAYMENT TIMEOUT

========== FILTER - ERROR LOGS ==========
ERROR Payment timeout

========== FLATMAP - WORDS ==========
ERROR
Payment
timeout

========== ERROR COUNT ==========
ERROR messages in this batch: 1
=================================
```

---

## 🧠 Key Concepts Learned

### DStream

A DStream is a continuous stream of data represented as a sequence of RDDs.

### StreamingContext

`StreamingContext` is used to configure and manage Spark Streaming applications.

### Batch Interval

The batch interval determines how frequently incoming data is collected and processed.

In this project:

```text
5 seconds
```

### Micro-Batch Processing

Incoming streaming data is divided into small batches and processed periodically.

### Socket Streaming

Spark can receive text data from a TCP socket using:

```scala
socketTextStream()
```

### Transformations

The project demonstrates:

```text
map()
filter()
flatMap()
reduce()
```

---

## 🏗️ Real-World Scenario

This project simulates **application log monitoring**.

A production application continuously generates logs:

```text
INFO User logged in
INFO Request received
ERROR Database connection failed
INFO Payment completed
ERROR API timeout
```

Spark Streaming can process these logs continuously and identify errors in near real time.

The same concept can be extended to:

- Server monitoring
- Application monitoring
- Security event processing
- Payment monitoring
- Error detection
- System health monitoring

---

## 📌 Important Note

The project uses `nc` to simulate a real-time log producer.

```text
Terminal 1
    ↓
Spark Streaming Application

Terminal 2
    ↓
nc -lk 9999
    ↓
Application Logs
```

Spark receives the logs through the TCP socket and processes them every 5 seconds.

---

## ✅ Day 23 Checklist

- [x] Create StreamingContext
- [x] Configure 5-second batch interval
- [x] Read TCP socket stream
- [x] Use `nc` for real-time input
- [x] Implement `map()`
- [x] Implement `filter()`
- [x] Implement `flatMap()`
- [x] Count ERROR messages
- [x] Demonstrate micro-batch processing
- [x] Process application logs
- [x] Verify real-time output

---

⭐ **Day 23 – Apache Spark DStreams Basics**
