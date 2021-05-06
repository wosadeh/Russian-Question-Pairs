"""
Utility file to download yandex Q question text by the URL
"""
import pandas as pd
from parsel import Selector
from urllib.request import urlopen
from typing import Optional

try:
    from tqdm.auto import tqdm
except ModuleNotFoundError:
    def tqdm(iterable, *args, **kwargs):
        return iterable


_not_found_texts = [
        'Кажется, этой страницы не\xa0существует',
        'Кажется, этой страницы не существует'
    ]


def get_q_text(url: str) -> Optional[str]:
    """Download Yandex Q question text by a given url

    Args:
        url: url of Yandex Q question

    Returns:
        A questions text
    """
    req = urlopen(url)
    charset = req.info().get_content_charset()
    page_text = req.read().decode(charset)

    sel = Selector(text=page_text, type='html')
    h1_text = sel.xpath('//h1/text()').getall()
    h1_text = list(filter(lambda s: s not in _not_found_texts, h1_text))
    if len(h1_text) == 0:
        return None
    else:
        return h1_text[0]


def extend_dataframe(df: pd.DataFrame, quite: bool = False) -> pd.DataFrame:
    """Adds Yandex Q question text to pandas DataFrame.
    DataFrame must contain 'url' column with corresponding question urls

    Args:
        df: pd.DataFrame input dateframe
        quite: If True, doesn't produce any output to stdout

    Returns:
        pandas.DataFrame: copy of input dataframe with column 'text', containing question text
    """
    extended_df = df.copy(deep=True)
    extended_df['text'] = [''] * len(extended_df)

    excluded = []
    if quite:
        index_iter = extended_df.index
    else:
        index_iter = tqdm(extended_df.index, unit='question', unit_scale=True)
    for i in index_iter:
        url = extended_df['url'].iat[i]
        q_text = get_q_text(url)
        if q_text is None:
            excluded.append(i)
        else:
            extended_df.at[extended_df.index == i, 'text'] = q_text

    return extended_df.drop(index=excluded)

