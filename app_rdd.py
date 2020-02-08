from operator import add

from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics

spark = SparkSession.builder.getOrCreate()



def read_csv(pth):
    return spark.read.csv(pth,
                          sep='\t',
                          header=True,
                          nullValue='\\N').rdd


def main():
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()

    titles_rdd = read_csv('./data/titles.tsv')
    ratings_rdd = read_csv('./data/ratings.tsv')
    principals_rdd = read_csv('./data/principals.tsv')
    names_rdd = read_csv('./data/names.tsv')
    episodes_rdd = read_csv('./data/episodes.tsv')
    crew_rdd = read_csv('./data/crew.tsv')
    akas_rdd = read_csv('./data/akas.tsv')

    print('titles after read: ', titles_rdd.getNumPartitions())
    print('ratings after read: ', ratings_rdd.getNumPartitions())
    print('principals after read: ', principals_rdd.getNumPartitions())
    print('names after read: ', names_rdd.getNumPartitions())
    print('episodes after read: ', episodes_rdd.getNumPartitions())
    print('crew after read: ', crew_rdd.getNumPartitions())
    print('akas after read: ', akas_rdd.getNumPartitions())

    titles_rdd = titles_rdd.filter(lambda r: False if r['genres'] is None else 'Comedy' in r['genres'].split(','))
    titles_rdd = titles_rdd.filter(lambda r: r['titleType'] != 'short')

    ratings_rdd = ratings_rdd.filter(lambda r: float(r['averageRating']) > 5.0)
    ratings_rdd = ratings_rdd.filter(lambda r: int(r['numVotes']) > 8376)

    principals_rdd = principals_rdd.filter(lambda r: r['category'] != 'self' and r['category'] != 'cinematographer')
    principals_rdd = principals_rdd.sortBy(lambda r: r['tconst'])
    principals_rdd = principals_rdd.filter(lambda r: int(r['ordering']) <= 3)

    names_rdd = names_rdd.filter(lambda r: r['deathYear'] is not None)
    names_rdd = names_rdd.filter(lambda r: False if r['primaryProfession'] is None else 'miscellaneous' not in r['primaryProfession'])

    akas_rdd = akas_rdd.filter(lambda r: r['isOriginalTitle'] == '1')
    akas_rdd = akas_rdd.filter(lambda r: r['region'] == 'US')

    full_movie_rdd = titles_rdd.join(akas_rdd).join(ratings_rdd).join(episodes_rdd)

    titles_grp_rdd = titles_rdd.groupBy(lambda r: None if r['genres'] is None else r['genres'].split(',')[0])

    full_genre_list = titles_rdd.map(lambda r: r['genres']).reduce(add)

    episode_mov_rdd = episodes_rdd.filter(lambda r: False if r['episodeNumber'] is None else int(r['episodeNumber']) > 10).join(titles_rdd)

    print(titles_rdd.count())
    print(ratings_rdd.count())
    print(principals_rdd.count())
    print(names_rdd.count())
    print(episodes_rdd.count())
    print(crew_rdd.count())
    print(akas_rdd.count())
    print(full_movie_rdd.count())
    print(episode_mov_rdd.count())
    print(titles_grp_rdd.count())

    print(titles_rdd.take(100))
    print(ratings_rdd.take(100))
    print(principals_rdd.take(100))
    print(names_rdd.take(100))
    print(episodes_rdd.take(100))
    print(crew_rdd.take(100))
    print(akas_rdd.take(100))
    print(full_movie_rdd.take(100))
    print(episode_mov_rdd.take(100))
    print(titles_grp_rdd.take(100))
    print(full_genre_list)

    print('titles after app: ', titles_rdd.getNumPartitions())
    print('ratings after app: ', ratings_rdd.getNumPartitions())
    print('principals after app: ', principals_rdd.getNumPartitions())
    print('names after app: ', names_rdd.getNumPartitions())
    print('episodes after app: ', episodes_rdd.getNumPartitions())
    print('crew after app: ', crew_rdd.getNumPartitions())
    print('akas after app: ', akas_rdd.getNumPartitions())

    stagemetrics.end()
    stagemetrics.print_report()


if __name__ == '__main__':
    main()
