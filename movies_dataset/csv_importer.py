import ast
import pandas as pd

from distutils.util import strtobool
from typing import Dict, List, Union


def convert_to_dict_list(string: str) -> Union[List[Dict], None]:
    if len(string) < 1:
        return None

    converted_value = ast.literal_eval(string)
    if type(converted_value) is list:
        return converted_value
    else:
        return None


def convert_to_dict(string: str) -> Union[Dict, None]:
    if len(string) < 1:
        return None

    converted_value = ast.literal_eval(string)
    if type(converted_value) is list and len(converted_value) == 1:
        return converted_value[0]
    elif type(converted_value) is dict:
        return converted_value
    else:
        return None


def format_imdb_id(id: str) -> str:
    if not id.startswith('tt'):
        return 'tt{}'.format(id)

    return id


def convert_to_bool(string: str) -> bool:
    try:
        converted_value = strtobool(string)
        return bool(converted_value)
    except:
        # print(string)
        return False


def convert_to_int(string: str) -> pd.np.int32:
    try:
        return pd.np.int32(string)
    except:
        # print(string)
        return pd.np.nan


def convert_to_float(string: str) -> pd.np.float:
    try:
        return pd.np.float(string)
    except:
        # print(string)
        return pd.np.nan


def format_str(string: str) -> Union[str, None]:
    try:
        if len(string) < 1:
            return None
        if 'NaN' in string:
            return None
        if string is None:
            return None

        return string
    except:
        print(string)
        return None


# credits.csv, keywords.csv and movies_metadata.csv contain "stringified" JSON Objects
# converter needed since default does not convert stringified JSON
credits_df = pd.read_csv('./credits.csv',
                         header=0, delimiter=',',
                         names=['cast', 'crew', 'tmdbId'],
                         converters={'cast': convert_to_dict_list,
                                     'crew': convert_to_dict_list,
                                     'tmdbId': convert_to_int})
print(credits_df)
credits_df.to_pickle('./credits.p')

# print(credits_df.iloc[0, 0][1]['name'])
# print(type(credits_df.iloc[0, 0][1]))

keywords_df = pd.read_csv('./keywords.csv',
                         header=0, delimiter=',',
                         names=['tmdbId', 'keywords'],
                         converters={'tmdbId': convert_to_int,
                                     'keywords': convert_to_dict_list})
print(keywords_df)
keywords_df.to_pickle('./keywords.p')
# print(keywords_df.iloc[0,0])

# https://stackoverflow.com/questions/43614377/pandas-read-csv-skiprows-not-working
# indices_to_skip = pd.np.array([19763, 29572, 35671])
# print(indices_to_skip)
movies_metadata_df = pd.read_csv('./movies_metadata.csv',
                                 header=0, delimiter=',',
                                 # iterator=True, chunksize=100, engine='python',
                                 # index_col=0, skiprows=indices_to_skip + 1,
                                 # error_bad_lines=False,
                                 parse_dates=True,
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
                                 converters={'adult': convert_to_bool,
                                             'belongs_to_collection': convert_to_dict,
                                             'budget': convert_to_int,
                                             'genres': convert_to_dict_list,
                                             'homepage': format_str,
                                             'tmdbId': convert_to_int,
                                             'imdbId': format_imdb_id,
                                             'original_language': format_str,
                                             'original_title': format_str,
                                             'overview': format_str,
                                             'popularity': convert_to_float,
                                             'poster_path': format_str,
                                             'production_companies': convert_to_dict_list,
                                             'production_countries': convert_to_dict_list,
                                             'release_date': format_str,
                                             'revenue': convert_to_float,
                                             'runtime': convert_to_float,
                                             'spoken_languages': convert_to_dict_list,
                                             'status': format_str,
                                             'tagline': format_str,
                                             'title': format_str,
                                             'video': convert_to_bool,
                                             'vote_average': convert_to_float,
                                             'vote_count': convert_to_int})
print(movies_metadata_df)
movies_metadata_df.to_pickle('./movies_metadata.p')
print(movies_metadata_df.iloc[19763, :])
print(movies_metadata_df.iloc[29572, :])
# print(type(movies_metadata_df.iloc[0, 0][1]))


# Simple CSV files

links_df = pd.read_csv('./links.csv',
                       header=0, delimiter=',',
                       names=['movieId', 'imdbId', 'tmdbId'],
                       converters={'movieId': convert_to_int,
                                   'imdbId': format_imdb_id,
                                   'tmdbId': convert_to_int})
print(links_df.describe())
links_df.to_pickle('./links.p')

links_small_df = pd.read_csv('./links_small.csv',
                             header=0, delimiter=',',
                             names=['movieId', 'imdbId', 'tmdbId'],
                             converters={'movieId': convert_to_int,
                                         'imdbId': format_imdb_id,
                                         'tmdbId': convert_to_int})
print(links_small_df.describe())
links_small_df.to_pickle('./links_small.p')

ratings_small_df = pd.read_csv('ratings_small.csv',
                               header=0, delimiter=',',
                               names=['userId', 'movieId', 'rating', 'timestamp'],
                               converters={'userId': convert_to_int,
                                           'movieId': convert_to_int,
                                           'rating': convert_to_float,
                                           'timestamp': convert_to_int})
print(ratings_small_df.describe())
ratings_small_df.to_pickle('./ratings_small.p')
