# Spark-Assignment

Spark-Assignment is a data tranformation project. Following steps were followed in it.

- Partition dataset 1 on gender
- Denormalize dataframe 1 to remove hierarchy and have a row for each phone number where name and gender are replicated for each phone number
- Perform left outer join - Dataset 1 to Dataset 2
- Add column called "total activity" with sum of the "posts" field
- Write final dataset output as parquet file

## Instructions to install and configure prerequisites or dependencies.

At the bare minimum you'll need the following for your development environment and little knowledge of linux commands:

1. [Python](http://www.python.org)(Would be good if you have Python 3.7.4 as I have tested it on Python 3.7.4)
2. [Java](https://www.java.com) (Pyspark has dependency of Java, I have installed java 8 in my system)
3. [Pyspark](https://pypi.org/project/pyspark/) (I have tested it on Pyspark 2.4.3)

## Local Setup
### Dependency Mangement and virtualenv for the project

python3 -m venv env

source env/bin/activate

pip3 install -r requirements.txt


### Generate Dataset 1
python3 create_dataset1.py

### Generate Dataset2 
python3 create_dataset2.py

### Run Spark Job
python3 spark_job.py

### Run all scripts sequentially
python3 app.py

## Output files
### final_list.parquet

Column Names

| gender| name| phone| posts| account| total_activity|

### summary_report.parquet

Column Names

| gender| name| total_activity|
