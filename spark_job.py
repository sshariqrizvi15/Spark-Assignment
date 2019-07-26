import pyspark
from pyspark.sql.functions import explode, array


def read_json(filename):
    return sqlContext.read.json(filename, multiLine=True)


def outer_join_two_datasets(dataset1, dataset2):
    return dataset1.join(dataset2, dataset1.name == dataset2.name, "left_outer"). \
        select(dataset1["*"], dataset2["posts"], dataset2["account"])


def summarise_report(summed_people_df):
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

# Add column called "total activity" with sum of the "posts" field
summed_post_df = joined_people_df.groupBy('name', 'phone').agg({'posts': 'sum'}).withColumnRenamed("SUM(posts)",
                                                                                                   "total_activity")

# Add remaining columns which were removed due to aggregation operation
joined_summed_people_df = joined_people_df.join(summed_post_df, joined_people_df.name == summed_post_df.name). \
    select(joined_people_df["*"], summed_post_df["total_activity"])

# Write detail dataframe to parquet fil
joined_summed_people_df.write.parquet("detail_report.parquet")
joined_summed_people_df.orderBy("name").show(40)

# Removed additional columns for Summarise Report
summary_df = summarise_report(joined_summed_people_df)

summary_df.write.parquet("summary_report.parquet")
summary_df.orderBy("name").show(200)

print("PySpark Job Completed.")
