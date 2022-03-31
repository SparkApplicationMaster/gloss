import sys

from sklearn import datasets
print(sys.executable)

import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]
import pyarrow as pa

from pyspark.sql import SparkSession, Window

from pyarrow.dataset import Expression, Dataset, field, dataset
import pyarrow.compute as pc
from timeit import default_timer as timer

data = "/home/x/data/"

class NamedExpression(Expression):
    def __init__(
        self, 
        exp#: Expression
    ):
         Expression.__init__()

    def alias(self, alias: str):
        self.alias = alias

def expr(exp):
    return exp


gloss = False
if not gloss:
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    spark = SparkSession\
            .builder\
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
            .config("spark.hadoop.dfs.client.read.shortcircuit.skip.checksum", "true")\
            .config("spark.sql.adaptive.enabled", "false")\
            .config("spark.sql.shuffle.partitions", "100")\
            .config("spark.driver.memory", "10g")\
            .getOrCreate()
else:
    from gloss.sql import *
    from gloss.sql.functions import *
    spark = SparkSession()


print(spark.read.format("parquet").load(data + "result_parquet").subtract(spark.read.format("parquet").load(data + "result_parquet_arrow")).count())

#print(spark.read.format("parquet").load(data + "result_parquet_arrow").count(), spark.read.format("parquet").load(data + "result_parquet").count())

# Generate a Pandas DataFrame
#pdf = pd.DataFrame(np.random.rand(1000000, 3))
#df = spark.read.parquet(data + "rand")
#df = spark.read.orc(data + "rand.orc")
#result_pdf = df.select("*").toPandas()
#df.printSchema()

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
#df = spark.createDataFrame(pdf).toDF("test1", "test2", "test3")

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow

start = timer()

#adf = spark.read.orc(data + "changed_orc/part-00001-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc")
#bdf = spark.read.orc(data + "changed_orc/part-00001-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc")

#print(adf.join(bdf, ["test1", "test2"]).repartition(1).toPandas())

#end = timer()
#print(end - start)
#import time
#time.sleep(10000)
#expr_abs_ge_test2
#exit(0)

import pyarrow
#print("pyarrow:", pyarrow.__version__)

#print(parq.read_table(f"/tmp/gloss_staging/stage=0", filters=[("in_task", "=", 0)]).drop(["out_task", "in_task"]))
#exit(1)
#data + "changed_orc/part-00000-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc", 

#print(dir(adf))
#print(adf.__pyx_vtable__.__repr__())

#exit(1)
#print(bdf.take([1,1,1,1,1,2,2,2,2,3,4,5,6]).to_pandas())

#print(bdf.to_pandas())
#true_arr = pa.chunked_array([pa.Array.from_pandas([True])])

#print(edf.schema)

import pyarrow.compute as pc

#a_index = []
#b_index = []

#adf = adf #.sort_by(join_keys_ordered)
#bdf = bdf #.sort_by(join_keys_ordered)

#import mmh3
#hash_col = pa.Array.from_pandas(np.mod(reduce(lambda a, b: np.add(a, b), map(lambda key: pd.util.hash_array(adf.column(key).to_pandas().to_numpy()), join_keys)), 200))

#adf = with_hash_mod(adf, "hash", join_keys, 200)

#ajf = with_row_number(adf.select(join_keys), "_join_rn_left", 0).combine_chunks().to_pandas()
#bjf = with_row_number(adf.select(join_keys), "_join_rn_right", 0).combine_chunks().to_pandas()

#join_res = pd.merge(
#    ajf,
#    bjf,
#    how="inner",
#    on=join_keys,
#    left_on=None,
#    right_on=None,
#    left_index=False,
#    right_index=False,
#    sort=False,
#    suffixes=("_x", "_y"),
#    copy=True,
#    indicator=False,
#    validate=None,
#)

#print(type(join_res["_join_rn_left"]))

                
#print(a_index, b_index)
#edf = join_row(adf.take(pa.Array.from_pandas(join_res["_join_rn_left"])), bdf.take(pa.Array.from_pandas(join_res["_join_rn_right"])))

#if len(edf):
#    edf = pa.concat_tables(edf)
#else:
#    edf = join_row(adf.slice(0, 0), bdf.slice(0, 0))
#print(edf.num_rows)
#print(edf.slice(965000, 30).to_pandas())

#adf = adf.to_pandas()
#bdf = bdf.to_pandas()

#end = timer()
#print(end - start)

#expr_abs_ge_test2
#exit(0)

#adf = spark.read.format("parquet")\
#.load(data + "changed_parquet_20")
#changed_orc/part-00001-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc

adf = spark.read.format("orc")\
.load(data + "small_orc")

bdf = spark.read.format("orc")\
.load(data + "small_orc")

adf = adf.join(bdf.withColumn("expr_abs_ge_test3", col("expr_abs_ge_test2")).select("test1", "expr_abs_ge_test3"), ["test1"], "inner")

#print(adf.count())
adf.repartition(20).write.mode("overwrite").format("parquet").save(data + "result_parquet")
#adf.write.mode("overwrite").format("parquet").partitionBy("expr_abs_ge_test2", "test_sign").save(data + "result_parquet")
#adf.repartition("expr_abs_ge_test2", "test_sign").write.mode("overwrite").format("parquet").partitionBy("expr_abs_ge_test2", "test_sign").save(data + "result_parquet_repart_by_cols")
#adf.repartition(10).write.mode("overwrite").format("parquet").save(data + "result_parquet_10")

end = timer()
print(end - start)

exit(0)
#.load(data + "rand")
#
#.load([data + "changed_orc/part-00000-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc", data + "changed_orc/part-00001-6022ecd0-a55c-4469-aa32-447f0773ff3c.snappy.orc", ])


print("count:")
print(adf.count())
print()

pa.jemalloc_set_decay_ms(0)



#print(adf._table.append_column("a", pc.scalar(0.1)))

#result_pdf = adf\
#.select("test1", "test2", "test3")\
#.withColumn("test_expr", col("test1") * col("test3") + col("test2") / -col("test3"))\
#.limit(10)\
#.toPandas()

try:
    result_df = adf\
    .select("test1", "test2", "test3")\
    .withColumn("test_expr", col("test1") * col("test3") + col("test2") / -col("test3"))\
    .withColumn("expr_abs_ge_test2", abs(col("test_expr")) >= col("test2"))\
    .withColumn("test_least_greatest", least("test1", coalesce(greatest("test2", "test3"), "test3"), abs("test_expr")))\
    .withColumn("test_sign", sign(col("test3") - col("test_least_greatest")))\
    .withColumn("t1r", round("test1", 1))\
    .withColumn("t2r", round("test1", 2))\
    .withColumn("t3r", round("test1", 3))\
    .withColumn("test_struct", struct("t1r", "t2r", "t3r"))\
    .drop("t1r", "t3r")\
    #.where(col("test1") > col("test2"))

    #result_pdf = result_df.limit(10).toPandas()
    #.count()
    #.toPandas()

    #print(result_pdf)

    end = timer()
    print(end - start)

    #result_df.repartition(20).write.mode("overwrite").format("orc").save(data + "result_orc")
    result_df.write.mode("overwrite").format("parquet").save(data + "result_parquet")

    end = timer()
    print(end - start)

    print(spark.jobs)
except Exception as e:
    print(e)

#print(result_pdf.head())

#.withColumn("test_str", lit("aaabb"))\

#adf._table.append_column("a", pa.ListArray(adf._table.column("test1"), adf._table.column("test2")))

#print(dir(pc))

#adf = pds.dataset(data + "rand", format="parquet")

#print(coalesce((col("a") + col("b") + col("c")) * col("d"), lit(0)))

#adf = spark.read.format("parquet").load(data + "rand")

#print(adf.count_rows())


#print(dir(pc))

#adt = adf._ds.to_table()


#print(adt.column("test2")._name)

#adt = adt.append_column("test_abs", pc.subtract(pc.add(pc.abs(adt.column("test3")), pc.abs(adt.column("test2"))), pc.abs(adt.column("test2"))))
#adt = adt.append_column("test_great", pc.greater(adt.column("test2"), pc.abs(adt.column("test_abs"))))
#adt = adt.append_column("test_struct", pc.make_struct(adt.column("test2"), pc.abs(adt.column("test_abs")), field_names=["test2", "test_abs"]))
#adt = adt.append_column("test_get_field", pc.struct_field(adt.column("test_struct"), [1]))

#print(dir(adt.column("test_struct")))

#print(dir(adt.column("test_struct").type))

#print(adt.column("test_struct").type.get_field_index("test_abs"))

#print("schema:")
#print(adt.schema)
#print()

#porc.write_table(adf.to_table(), data + "rand.orc")



#result_pdf = adf\
#    .withColumn("test4", col("test1") > 0.5)\
#    .toPandas()


#result_pdf = adt.to_pandas()

#result_pdf = adf.to_table().to_pandas()





#print(col("a") + col("b") / col("e") + col("c") * -col("d"))
#print(adt.column_names)

#df.repartition(10).write.mode("overwrite").parquet(data + "rand")


#print(dir(pyspark))