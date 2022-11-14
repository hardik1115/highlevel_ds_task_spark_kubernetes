import sys
import os
import time
#import mlflow
#import wandb
import pickle
from random import random
from operator import add

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def clean_encode_data(df, drop_cols, encode_cols):
    '''
    Function to drop columns, encode categorical columns and correct dtypes.
    '''

    df = df.dropna()
    
    for col_ in encode_cols: 
        indexer = StringIndexer(inputCol=col_, outputCol='encoded_'+col_) 
        df = indexer.fit(df).transform(df) 
    
    cols_to_drop = drop_cols + encode_cols
    df = df.drop(*cols_to_drop)

    df = df.select(*(col(c).cast("float").alias(c) for c in df.columns))

    return df
    
def prep_data(df):
    '''
    Function to format data to feed into a pyspark.ml model instance using VectorAssembler and splitting into training and validation sets.
    '''
    feature_cols = sorted(list(df.columns))
    
    try:
        feature_cols.remove('resale_price')
    except:
        pass

    vectorAssembler = VectorAssembler(inputCols = feature_cols, outputCol = 'features')
    v_df = vectorAssembler.transform(df)

    try:
        v_df = v_df.select(col('features'), col('resale_price').alias('label'))
    except:
        pass


    return v_df

def train_val_model(df, model_instance, test_proportion):
    '''
    Train and validate the model and print the results.
    '''

    splits = df.randomSplit([1-test_proportion, test_proportion])
    train_df = splits[0]
    val_df = splits[1]

    start = time.time()
    lrModel = model_instance.fit(train_df)
    stop = time.time()


    
    val_result = lrModel.evaluate(val_df)
    trainingSummary = lrModel.summary

    print("\nValidation Data results:\n")
    print("MSE on val data = %g" % val_result.meanSquaredError)
    print("r2 on val data = %g" % val_result.r2)
    print("\nTraining Data results:\n")
    print("MSE: %f" % trainingSummary.meanSquaredError)
    print("r2: %f" % trainingSummary.r2)
    
    time_taken = stop - start
    print(f"Training time: {time_taken}s")

    return lrModel

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[1]").appName("LinearReg_Housing_Task").getOrCreate()

    df = spark.read.options(mode="DROPMALFORMED", header= True).csv('../data/mllib/Training Set.csv')
    test_df = spark.read.options(mode="DROPMALFORMED", header= True).csv('../data/mllib/Testing Set Without Value.csv')
    
    drop_cols = ['town', 'block', 'street_name', 'address', 'flat_model', 'month']
    encode_cols = ['storey_range', 'flat_type']

    df = clean_encode_data(df = df, drop_cols = drop_cols, encode_cols = encode_cols)
    test_df = clean_encode_data(df = test_df, drop_cols = drop_cols, encode_cols = encode_cols)

    df = prep_data(df)
    test_df = prep_data(test_df)

    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    trained_model = train_val_model(df = df, model_instance = lr, test_proportion = 0.3)

    predicted_prices = trained_model.transform(test_df)

    spark.stop()
