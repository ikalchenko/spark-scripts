from pyspark.sql import SparkSession, functions as f
from operator import add
from sparkmeasure import StageMetrics


spark = SparkSession.builder.getOrCreate()


def read_csv(pth):
    return spark.read.csv(pth,
                          sep='\t',
                          header=True,
                          nullValue='\\N')


def main():
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()

    titles_df = read_csv('./data/titles.tsv')
    ratings_df = read_csv('./data/ratings.tsv')
    principals_df = read_csv('./data/principals.tsv')
    names_df = read_csv('./data/names.tsv')
    episodes_df = read_csv('./data/episodes.tsv')
    crew_df = read_csv('./data/crew.tsv')
    akas_df = read_csv('./data/akas.tsv')

    titles_df = titles_df.filter(f.array_contains(f.split(f.col('genres'), ','), 'Comedy') & (f.col('titleType') != 'short'))

    ratings_df = ratings_df.filter(f.col('averageRating') > 5.0)
    ratings_df = ratings_df.filter(f.col('numVotes').cast('int') > 8376)

    principals_df = principals_df.filter((f.col('category') != 'self') & (f.col('category') != 'cinematographer'))
    principals_df = principals_df.orderBy('tconst')
    principals_df = principals_df.filter(f.col('ordering').cast('int') <= 3)

    names_df = names_df.filter(f.col('deathYear').isNotNull())
    names_df = names_df.filter(f.array_contains(f.split(f.col('primaryProfession'), ','), 'miscellaneous'))

    akas_df = akas_df.filter(f.col('isOriginalTitle') == '1')
    akas_df = akas_df.filter(f.col('region') == 'US')

    full_movie_df = titles_df.join(akas_df, f.col('tconst') == f.col('titleId'), 'full').join(ratings_df, on=['tconst']).join(episodes_df, on=['tconst'])

    titles_grp_df = titles_df.withColumn('genres', f.explode(f.split(f.col('genres'), ','))).groupBy('genres').agg(f.count('tconst'))

    episode_mov_df = episodes_df.filter(f.col('episodeNumber').cast('int') > 10).join(titles_df, on=['tconst'])

    print(titles_df.count())
    print(ratings_df.count())
    print(principals_df.count())
    print(names_df.count())
    print(episodes_df.count())
    print(crew_df.count())
    print(akas_df.count())
    print(full_movie_df.count())
    print(episode_mov_df.count())
    print(titles_grp_df.count())

    print(titles_df.show(100))
    print(ratings_df.show(100))
    print(principals_df.show(100))
    print(names_df.show(100))
    print(episodes_df.show(100))
    print(crew_df.show(100))
    print(akas_df.show(100))
    print(full_movie_df.show(100))
    print(episode_mov_df.show(100))
    print(titles_grp_df.show(100))

    stagemetrics.end()
    stagemetrics.print_report()


if __name__ == '__main__':
    main()
