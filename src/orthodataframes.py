import pyspark
from pyspark.sql import functions as F
import pyspark.sql.types as sq_types
from orthoworkflow import pull_input_tile, src_tile_id

import numpy as np

x_to_tile = lambda x: src_tile_id("",0,int(x),int(x),"in")

def _make_key_rdd(sc):
    # Make a cached performant RDD to start with
    keys_rdd = sc.parallelize(range(101),20).map(x_to_tile).cache()
    _ = keys_rdd.collect()
    return keys_rdd

def make_img_df(sqlContext, keys_rdd):
    kmeta_df = sqlContext.createDataFrame(keys_rdd.map(lambda x: x._asdict()))
    # applying python functions to DataFrames is more difficult and requires using typed UDFs
    twod_arr_type = sq_types.ArrayType(sq_types.ArrayType(sq_types.IntegerType()))
    # the pull_input_tile function is wrapped into a udf to it can be applied to create the new image column
    # numpy data is not directly supported and typed arrays must be used instead therefor we run the .tolist command
    pull_tile_udf = F.udf(lambda x: pull_input_tile(x_to_tile(x)).tolist(), returnType = twod_arr_type)
    kimg_df = kmeta_df.withColumn('Image', pull_tile_udf(kmeta_df['x']))

    s_query = kimg_df.where(kimg_df['x']>99)
    return s_query.show()

def make_img_table(img_df):
    """
    Makes a table with actual image data in SparkSQL format.
    It also creates a UDF for finding the max point
    :param img_df: the dataframe to start with (based on numpy data)
    :return:
    """
    pt_2d_type = sq_types.StructType(fields=[sq_types.StructField("_1", sq_types.IntegerType()),
                                             sq_types.StructField("_2", sq_types.IntegerType())])
    brightest_point_udf = F.udf(lambda x: np.unravel_index(np.argmax(x), dims=np.shape(x)), returnType=pt_2d_type)
    mean_point_udf = F.udf(lambda x: float(np.mean(x)), returnType=sq_types.DoubleType())
    kimg_max_df = img_df.withColumn('MeanPoint', mean_point_udf(img_df['Image']))
    return kimg_max_df
