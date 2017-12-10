# Running csv_importer

Default is running in current directory.

## Datasets

Imports [Kaggle movies dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset),
converts to Pandas dataframes with cleanup and exports as pickle files.

Script expects all 6 datasets (*credits.csv *, *keywords.csv*, *links.csv*, *links_small.csv*, *movies_metadata.csv*, *ratings_small.csv*).


## Setting path to movies datasets.

```python
python csv_importer.py --path /home/ayla/movies
```

## Usage

```python
python csv_importer.py --help
```
