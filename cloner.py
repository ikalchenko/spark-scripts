from pyspark.sql import SparkSession, functions as f
from operator import add
import os
import shutil


spark = SparkSession.builder.getOrCreate()

CLONE_FACTOR = 1


def read_csv(pth):
    return spark.read.csv(pth,
                          sep='\t',
                          header=True,
                          nullValue='\\N')


def main():
    all_dfs = {'titles': read_csv('./data/titles.tsv'),
               'ratings': read_csv('./data/ratings.tsv'),
               'principals': read_csv('./data/principals.tsv'),
               'names': read_csv('./data/names.tsv'),
               'episodes': read_csv('./data/episodes.tsv'),
               'crew': read_csv('./data/crew.tsv'),
               'akas': read_csv('./data/akas.tsv')}

    for name, df in all_dfs.items():
        for i in range(CLONE_FACTOR - 1):
            df1 = all_dfs[name].withColumn(all_dfs[name].columns[0], f.concat(f.col(all_dfs[name].columns[0]), f.lit('1')))
            all_dfs[name] = df.unionAll(df1)
        (all_dfs[name].repartition(1)
            .write.format("com.databricks.spark.csv")
            .option("header", "true")
            .option('delimiter', '\t')
            .option('emptyValue', r'\N')
            .save("data_x{}/{}.tsv".format(CLONE_FACTOR, name)))

if __name__ == '__main__':
    main()
