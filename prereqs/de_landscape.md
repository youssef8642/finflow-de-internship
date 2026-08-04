# Data Engineering Landscape

## 1. What is a Data Engineer?

A Data Engineer is responsible for collecting raw data from different sources and creating a pipeline which will process and organize the data for the next step in the business.

In a real financial company, raw data can come from sources such as user transactions, ATM machine activities, and other transaction sources. This data can be unfiltered and uneven, and can come from different sources. The Data Engineer's role is to build a pipeline that will organize, clean, transform, and prepare this data for the next stages of the business.

### Data Engineer vs Data Scientist vs Software Engineer

A Data Engineer would build a pipeline to process and organize the data.

A Data Scientist would analyze the data and try to detect where the fraud happens according to transaction trends or patterns. For example, after receiving transaction data, the Data Scientist can analyze the transaction patterns and look for suspicious behavior.

A Software Engineer would build the APIs and platforms which will be used. In a fraud detection system, they could build the API and platform that will be used by the system.

Therefore, the three roles are connected because the Data Engineer prepares the data, the Data Scientist analyzes the data, and the Software Engineer builds the APIs and platforms which will be used.

---

## 2. ETL vs ELT

ETL stands for Extract, Transform, and Load.

The Extract step means getting the raw data from sources such as user transactions, ATM activities, and other sources.

The Transform step means organizing and standardizing the data. For example, transaction data can be transformed into a standard so that it can be compared against other columns fairly.

The Load step means having a destination such as a warehouse or another storage server to store this data.

A financial-company ETL example would be extracting raw data from user transactions and ATM machine activities, transforming it into a standard format, and then loading it into a storage server such as a data warehouse.

ELT stands for Extract, Load, and Transform.

The main difference from ETL is that the raw data is loaded first and transformed afterward inside the destination system.

For example, a bank could first extract the raw transaction data and load it into a warehouse, and then transform the data inside the warehouse.

One advantage of this approach is that the warehouse could potentially have the computational power to transform the raw data. It can also allow the original raw data to be kept so that different transformations can be done later.

Therefore:

ETL = Extract → Transform → Load

ELT = Extract → Load → Transform


---

## 3. Batch vs Streaming

Batch processing means collecting data and processing it at scheduled intervals.

For example, a bank could collect all transactions during the day and process them together at the end of the day. This could help the company obtain the day's patterns for context and identify suspicious patterns or anomalies.

Streaming is different because the data is processed as it arrives.

For example, a certain customer could make a purchase for a small or cheap product. Then, within the next 5 minutes, large transactions occur from his account, which leads to suspicion. A streaming system could detect this behavior as the transactions arrive instead of waiting until the end of the day.

The main difference is that batch processing processes accumulated data periodically, while streaming processes data continuously as it arrives.

---

## 4. Why Pipelines Need Parallelism

If a pipeline processes every task sequentially, it takes longer to complete a simple task which can be done in parallel.

For example, if downloading one dataset takes 20 seconds, another takes 10 seconds, and another takes 15 seconds, processing them one after another would take about 45 seconds.

If these tasks are independent, they can instead run at the same time. This means the total processing time can be closer to the longest individual task, which in this example would be around 20 seconds.

This becomes important in Data Engineering because pipelines can have hundreds or thousands of independent tasks. Processing everything sequentially would not scale well, and as the number of tasks increases, the total execution time grows linearly, making the system slow and inefficient.

To solve this, Python provides concurrency tools that allow multiple tasks to run at the same time depending on the type of workload:

Threading is used for I/O-bound tasks such as downloading data or calling APIs, where most of the time is spent waiting.
Multiprocessing is used for CPU-bound tasks such as heavy calculations, where multiple CPU cores can be used in parallel.
Asyncio is used for managing many I/O-bound tasks efficiently using a single event loop without creating multiple threads.

By using these tools, Data Engineering pipelines can process large-scale data much faster and handle many independent operations efficiently.

---

## 5. Data Engineering Stack

A basic Data Engineering pipeline can be represented as:

Source
   ↓
Ingest
   ↓
Transform
   ↓
Model
   ↓
Serve


### Source

The Source is where the raw data comes from. Examples include ATMs, user transactions, and other sources associated with gathering data.

### Ingest

Ingest means collecting or importing the data into the pipeline. The purpose is to get the data into the pipeline so it can be processed.

### Transform

Transform means standardizing and organizing the data. This can include cleaning the data and making it follow a standard so it can be used properly.

### Model

Model means structuring the transformed data into a useful structure for analysis. The data can be organized into useful tables or models.

### Serve

Serve means giving the prepared data to the next users or systems. The prepared data can then be used by Data Scientists, analysts, applications, or other parts of the business.

Overall, the pipeline takes raw data from sources such as transactions and ATM activities and turns it into organized and prepared data that can be used for the next step in the business.
