"""
    spark_job is running a pyspark job to process two data sets into desirable report
"""

import pyspark
from pyspark.sql.functions import explode, array


def read_json(filename):
    """Read Json File

    Parameters:
    filename (string): Name of a file
   """

    return sqlContext.read.json(filename, multiLine=True)


def outer_join_two_datasets(dataset1, dataset2):
    """Join two datasets

    Parameters:
    dataset1 (dataframe): Dataframe with whom another will join
    dataset2 (dataframe): Dataframe to be joined

    Returns:
    dataframe: Returning a joined dataframe

   """

    return dataset1.join(dataset2, dataset1.name == dataset2.name, "left_outer"). \
        select(dataset1["*"], dataset2["posts"], dataset2["account"])


def summarise_report(summed_people_df):
    """Removing extra columns and duplicated rows

    Parameters:
    dataset1 (dataframe): Data frame to whom operation will perform

    Returns:
    dataframe: Returning a summarised data frame

   """
    rel_col_df = summed_people_df.drop('phone', 'posts', 'account')
    return rel_col_df.drop_duplicates()


sc = pyspark.SparkContext("local", "count app")
sqlContext = pyspark.SQLContext(sc)

people_df = read_json("dataset1.json")
post_df = read_json("dataset2.json")

# Denormalize data frame to remove hierarchy and have a row for each phone number
people_denorm_df = people_df.repartition('gender').select('name', 'gender', explode(array("phone.*")).alias('phone'))

# Left outer join people_denorm_df with post_df on name. So names not present in dataset2 could be present in a new
# dataset.
joined_people_df = outer_join_two_datasets(people_df, post_df)

na_fill_joined_df = joined_people_df.fillna({'posts': '0'})
# Add column called "total activity" with sum of the "posts" field
summed_post_df = na_fill_joined_df.groupBy('name', 'phone').agg({'posts': 'sum'}).withColumnRenamed("SUM(posts)",
                                                                                                 "total_activity")

# Add remaining columns which were removed due to aggregation operation
joined_summed_people_df = na_fill_joined_df.join(summed_post_df, na_fill_joined_df.name == summed_post_df.name). \
    select(na_fill_joined_df["*"], summed_post_df["total_activity"])

# Write detail dataframe to parquet fil
joined_summed_people_df.write.parquet("detail_report.parquet")

# Removed additional columns for Summarise Report
summary_df = summarise_report(joined_summed_people_df)

summary_df.write.parquet("summary_report.parquet")

print("PySpark Job Completed.")
