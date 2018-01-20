import argparse
import ast
import pandas as pd

from datetime import datetime
from distutils.util import strtobool
from typing import Dict, List, Union


def convert_to_dict_list(string: str) -> Union[List[Dict], None]:
    """
    Use Python's Active Syntax Tree (ast) to convert strings to a list of dict objects.
    This is an easy way to deal with the lists and the single-quoted dicts, which the JSON converter won't handle.
    """
    # initial sanity check for malformed input: need minimum 2 characters to form valid dict object
    if len(string) <= 1:
        return None

    try:
        converted_value = ast.literal_eval(string)
        if type(converted_value) is list:
            return converted_value
        else:
            return None
    except ValueError:
        return None


def convert_to_dict(string: str) -> Union[Dict, None]:
    """
    Use Python's Active Syntax Tree (ast) to convert strings to dict objects.
    This is an easy way to deal with the single-quoted dicts, which the JSON converter won't handle.
    """
    # initial sanity check for malformed input: need minimum 2 characters to form valid dict object
    if len(string) <= 1:
        return None

    try:
        converted_value = ast.literal_eval(string)
        if type(converted_value) is list and len(converted_value) == 1:
            return converted_value[0]
        elif type(converted_value) is dict:
            return converted_value
        else:
            return None
    except ValueError:
        return None


def convert_to_bool_object(string: str) -> pd.np.object:
    try:
        converted_value = strtobool(string)
        return pd.np.bool(converted_value)
    except:
        # promotes to object
        return pd.np.nan


def convert_to_float(string: str) -> pd.np.float:
    try:
        return pd.np.float(string)
    except:
        return pd.np.nan


def cleanup_str(string: str) -> str:
    """Basic string cleanup. If the string matches any empty or null patterns, return empty string."""
    try:
        if len(string) < 1 or 'NaN' in string or string is None:
            return ''
        return string
    except:
        return ''


def convert_to_date(string: str) -> Union[datetime, None]:
    try:
        result = datetime.strptime(string, '%Y-%m-%d')
        return result
    except:
        return None


def convert_to_imdb_id(id: str) -> str:
    """Format IMDB IDs to follow the same ID convention (starts with 'tt'."""
    try:
        id = cleanup_str(id)
        if not id.startswith('tt'):
            return 'tt{}'.format(id)
        return id
    except:
        return None


def convert_to_id(id: str) -> pd.np.int32:
    try:
        return pd.np.int32(id)
    except:
        return -1


def credits_array_to_dataframe(array: pd.np.ndarray, id_array: pd.np.ndarray) -> pd.DataFrame:
    accumulator = list()

    for i in range(len(array)):
        df = pd.DataFrame(array[i])
        df['tmdbId'] = id_array[i]
        accumulator.append(df)

    return pd.concat(accumulator, axis=0, ignore_index=True)


def main(path: str) -> None:
    # credits.csv, keywords.csv and movies_metadata.csv contain "stringified" JSON Objects
    # converter needed since default does not convert stringified JSON
    credits_df = pd.read_csv('{}/credits.csv'.format(path),
                             header=0, delimiter=',',
                             names=['cast', 'crew', 'tmdbId'],
                             converters={'cast': convert_to_dict_list,
                                         'crew': convert_to_dict_list,
                                         'tmdbId': convert_to_id})
    print(credits_df.dtypes)
    print(credits_df.info(memory_usage='deep'))
    credits_df.to_pickle('{}/credits.p'.format(path))

    cast_credits_df = credits_array_to_dataframe(credits_df['cast'].values, credits_df['tmdbId'].values)
    cast_credits_df['cast_id'] = cast_credits_df.cast_id.astype(int)
    cast_credits_df['gender'] = cast_credits_df.gender.astype(int)
    cast_credits_df['id'] = cast_credits_df.id.astype(int)
    cast_credits_df['order'] = cast_credits_df.order.astype(int)
    print(cast_credits_df.info(memory_usage='deep'))
    cast_credits_df.to_pickle('{}/cast_credits.p'.format(path))

    crew_credits_df = credits_array_to_dataframe(credits_df['crew'].values, credits_df['tmdbId'].values)
    crew_credits_df['gender'] = crew_credits_df.gender.astype(int)
    crew_credits_df['id'] = crew_credits_df.id.astype(int)
    print(crew_credits_df.info(memory_usage='deep'))
    crew_credits_df.to_pickle('{}/crew_credits.p'.format(path))

    keywords_df = pd.read_csv('{}/keywords.csv'.format(path),
                              header=0, delimiter=',',
                              names=['tmdbId', 'keywords'],
                              converters={'tmdbId': convert_to_id,
                                          'keywords': convert_to_dict_list})
    print(keywords_df.dtypes)
    print(keywords_df.info(memory_usage='deep'))
    keywords_df.to_pickle('{}/keywords.p'.format(path))

    movies_metadata_df = pd.read_csv('{}/movies_metadata.csv'.format(path),
                                     header=0, delimiter=',',
                                     names=['adult',
                                            'belongs_to_collection',
                                            'budget',
                                            'genres',
                                            'homepage',
                                            'tmdbId',
                                            'imdbId',
                                            'original_language',
                                            'original_title',
                                            'overview',
                                            'popularity',
                                            'poster_path',
                                            'production_companies',
                                            'production_countries',
                                            'release_date',
                                            'revenue',
                                            'runtime',
                                            'spoken_languages',
                                            'status',
                                            'tagline',
                                            'title',
                                            'video',
                                            'vote_average',
                                            'vote_count'],
                                     converters={'adult': convert_to_bool_object,
                                                 'belongs_to_collection': convert_to_dict,
                                                 'budget': convert_to_float,
                                                 'genres': convert_to_dict_list,
                                                 'homepage': cleanup_str,
                                                 'tmdbId': convert_to_id,
                                                 'imdbId': convert_to_imdb_id,
                                                 'original_language': cleanup_str,
                                                 'original_title': cleanup_str,
                                                 'overview': cleanup_str,
                                                 'popularity': convert_to_float,
                                                 'poster_path': cleanup_str,
                                                 'production_companies': convert_to_dict_list,
                                                 'production_countries': convert_to_dict_list,
                                                 'release_date': convert_to_date,
                                                 'revenue': convert_to_float,
                                                 'runtime': convert_to_float,
                                                 'spoken_languages': convert_to_dict_list,
                                                 'status': cleanup_str,
                                                 'tagline': cleanup_str,
                                                 'title': cleanup_str,
                                                 'video': convert_to_bool_object,
                                                 'vote_average': convert_to_float,
                                                 'vote_count': convert_to_float})

    # cleanup malformed rows
    null_adult_cols = movies_metadata_df.loc[movies_metadata_df['adult'].isnull()].index.values
    movies_metadata_df = movies_metadata_df.drop(null_adult_cols)
    movies_metadata_df['adult'] = movies_metadata_df.adult.astype(bool)
    movies_metadata_df['video'] = movies_metadata_df.video.astype(bool)

    print(movies_metadata_df.dtypes)
    print(movies_metadata_df.info(memory_usage='deep'))
    movies_metadata_df.to_pickle('{}/movies_metadata.p'.format(path))

    # Simple CSV files

    links_df = pd.read_csv('{}/links.csv'.format(path),
                           header=0, delimiter=',',
                           names=['movieId', 'imdbId', 'tmdbId'],
                           converters={'movieId': convert_to_id,
                                       'imdbId': convert_to_imdb_id,
                                       'tmdbId': convert_to_id})
    print(links_df.dtypes)
    print(links_df.info(memory_usage='deep'))
    links_df.to_pickle('{}/links.p'.format(path))

    links_small_df = pd.read_csv('{}/links_small.csv'.format(path),
                                 header=0, delimiter=',',
                                 names=['movieId', 'imdbId', 'tmdbId'],
                                 converters={'movieId': convert_to_id,
                                             'imdbId': convert_to_imdb_id,
                                             'tmdbId': convert_to_id})
    print(links_small_df.dtypes)
    print(links_small_df.info(memory_usage='deep'))
    links_small_df.to_pickle('{}/links_small.p'.format(path))
    # links_small_df.to_csv('{}/links_small_clean.csv'.format(path))

    ratings_small_df = pd.read_csv('{}/ratings_small.csv'.format(path),
                                   header=0, delimiter=',',
                                   names=['userId', 'movieId', 'rating', 'timestamp'],
                                   converters={'userId': convert_to_id,
                                               'movieId': convert_to_id,
                                               'rating': convert_to_float,
                                               'timestamp': convert_to_float})
    print(ratings_small_df.dtypes)
    print(ratings_small_df.info(memory_usage='deep'))
    ratings_small_df.to_pickle('{}/ratings_small.p'.format(path))


if '__main__' == __name__:
    parser = argparse.ArgumentParser(description='Load and import movies data files to Pandas dataframes.'
                                                 'Load files from and output picked dataframes to PATH. Default is "."')
    parser.add_argument('--path', default='.', required=False)
    args = parser.parse_args()
    main(args.path)
