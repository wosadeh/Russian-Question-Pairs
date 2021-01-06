import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pymystem3 import Mystem
from nltk import word_tokenize
from nltk import download as nltk_download

from unicodedata import normalize
from typing import Sequence, List, Union, Any, Callable, Optional, Tuple
from copy import copy
from os import path

nltk_download('punkt', quiet=True)

# Unicode symbols which can be replaced by ASCII char without any ambiguity
_char_replacement = [
    ('\t', ' '),
    ('\n', ' '),
    ('\r', ' '),
    ('\xa0', ' '),
    ('\u200b', ' '),
    ('\u200c', ' '),
    ('\u2028', ' '),
    ('»', '"'),
    ('«', '"'),
    ('“', '"'),
    ('”', '"'),
    ('„', '"'),
    ('‘', "'"),
    ('’', "'"),
    ('‐', '-'),
    ('–', '-'),
    ('—', '-'),
    ('―', '-'),
    ('−', '-'),
]
_substr_replacement = [
    ('\\r\\n', ' '),
    ('\\r', ' '),
    ('\\n', ' '),
    # We compose 'й' again after unicode decomposition,
    # cause models like BERT usually understands words with this cyrillic letter.
    # But we don't the same thing for 'ё', cause in russian language it's common to replace it by 'е' letter
    # As a result models may incorrectly process words with 'ё' letter
    ('й', 'й'),  # The first string is a two characters (decomposition of й)
    ('Й', 'Й')
]

_char_replace_dict = dict(_char_replacement)

# Unicode symbol, which can be removed without doubt
_chars_soft_remove = ['`', '\xad', '´', '¶', '′', '\u200d']
_chars_soft_remove_set = set(_chars_soft_remove)

# Unicode symbols, which can be removed from string, but string meaning may be distorted
_chars_hard_remove = [
    '©', '®', '•', '†', '∙', '\u2061',
    '̀', '́', '̂', '̃', '̄', '̅', '̆', '̇', '̈', '̉', '̊', '̋', '̌', '̍', '̎', '̏', '̐', '̑', '̒', '̓',
    '̔', '̕', '̚', '̢', '̣', '̧', '̨', '̯', '̶', '̸', '̽', '̾', '̿', '͂', '͆', '͊', '͋', '͌', '͏', '͐', '͒', '͗', '͛',
    '͜',
    '͝', '͡', 'ͦ', 'ͨ', 'ͪ', 'ͫ', 'ͬ', 'ͭ', 'ͮ',
]
_chars_hard_remove_set = set(_chars_hard_remove)

with open('obscene_words.txt') as f:
    obscene_words = list(map(lambda s: s.rstrip(), f))
_obscene_words_set = set(obscene_words)


def sanitize_unicode(s: str, mode: str = 'soft') -> str:
    # Normalizing unicode and remove meaningless characters
    if mode == 'soft':
        chars2remove = _chars_soft_remove_set
    elif mode == 'hard':
        chars2remove = _chars_soft_remove_set | _chars_hard_remove_set
    else:
        raise ValueError("Only 'hard' and 'soft' mode are supported")

    normalized_s = normalize('NFKD', s)
    sanitized_s = ''.join(char for char in normalized_s if char not in chars2remove)
    for args in _substr_replacement:
        sanitized_s = sanitized_s.replace(*args)

    sanitized_s = ''.join(_char_replace_dict.get(char, char) for char in sanitized_s)
    return sanitized_s


def is_valid_unicode_range(s: str, ranges: Union[None, Sequence[Tuple[int, int]], Tuple[int, int]] = None) -> bool:
    if ranges is None:
        valid_ranges = [
            (0x0000, 0x007f),  # ASCII
            (0x0400, 0x04ff),  # Cyrillic
            # (0x1f300, 0x1f6fc)  # Emojis
        ]
    else:
        if isinstance(ranges, Tuple):
            valid_ranges = [ranges]
        else:
            valid_ranges = ranges

    is_valid = True
    for unicode_number in map(ord, s):
        char_valid = False
        for range_min, range_max in valid_ranges:
            char_valid |= range_min <= unicode_number <= range_max
        is_valid &= char_valid
    return is_valid


_mystem_analyzer = Mystem()


def lemmatize_all(sentences: Sequence[str], chunk_size: int = 1000) -> List[List[str]]:
    result = []
    cur_sent_tokens = []
    for i in range(0, len(sentences), chunk_size):
        chunk = sentences[i:i + chunk_size]
        prev_res_size = len(result)
        for token in _mystem_analyzer.lemmatize('\n'.join(chunk)):
            stripped_tok = token.rstrip('\n')
            if stripped_tok != token:
                # Token contains \n char, which was added manually as sentence separator
                cur_sent_tokens.append(stripped_tok)
                result.append(copy(cur_sent_tokens))
                cur_sent_tokens = []
            else:
                cur_sent_tokens.append(token)
        if len(cur_sent_tokens) > 0:
            result.append(copy(cur_sent_tokens))
            cur_sent_tokens = []
        if len(result) - prev_res_size != len(chunk):
            raise RuntimeError('Some error occurred during in chunking pipeline while lemmatizing text with MyStem.')

    return result


def obscene_filter(sent: Union[Sequence[str], str], is_input_lemmatized: bool = False) -> bool:
    if not is_input_lemmatized:
        if isinstance(sent, Sequence):
            sent = ' '.join(sent)
        sent = lemmatize_all([sent], chunk_size=1)

    if isinstance(sent, str):
        sent = word_tokenize(sent)

    for token in sent:
        if token in _obscene_words_set:
            return False
    return True


def apply_index_filter(idx: Sequence[int], collection: Sequence[Any], filter: Callable[[Any], bool],
                       filter_name: Optional[str] = None) -> List[int]:
    if filter_name is None:
        if hasattr(filter, '__name__'):
            filter_name = filter.__name__
        else:
            filter_name = ''

    invalid_idx = []
    valid_idx = []
    for i in idx:
        if filter(collection[i]):
            valid_idx.append(i)
        else:
            invalid_idx.append(i)

    print(f'Applying filter {filter_name}:')
    print('\t# of item before filtering: {}'.format(len(idx)))
    print('\t# of filtered out items: {}'.format(len(invalid_idx)))
    print('\t# of remaining items: {}'.format(len(valid_idx)))
    return valid_idx


class QIdDataError(ValueError):
    pass


MIN_LEN = 6
MAX_LEN = 14
IPM_LOWER_THRESHOLD = 2.
questions_data_path = '../All_questions_with_tags.csv'
_required_columns = ['id', 'short_name', 'url']

if __name__ == '__main__':
    # Resolving correct path to questions csv
    while not path.isfile(questions_data_path):
        questions_data_path = input('Enter path to CSV file with question URLs: ')

    questions = pd.read_csv(questions_data_path, sep=';')

    # Checking file correctness
    if len(set(_required_columns) - set(questions.columns)) > 0:
        error_msg = 'Following columns are required: '
        for col in _required_columns:
            error_msg += f'{col}, '
        raise QIdDataError(error_msg[:-1])

    if questions['id'].dtype not in [np.int8, np.int16, np.int32, np.int64, np.int, np.uint8, np.uint16, np.uint32,
                                     np.uint64, np.uint]:
        raise QIdDataError('id column may contain only integers')

    if len(set(questions['id'])) < len(questions):
        raise QIdDataError('Some question id is not unique')

    if len(set(questions['short_name'])) < len(questions):
        raise QIdDataError('Some question short name is not unique')

    text_was_presented = True
    # Download questions text if not presented
    if 'text' not in questions:
        text_was_presented = False
        questions['text'] = [''] * len(questions)
        # TODO add questions text downloading
        raise NotImplementedError

    print('Normalizing unicode representation of text questions')
    questions['text'] = questions['text'].apply(lambda s: sanitize_unicode(s, mode='hard'))

    cur_idx = list(questions.index)
    print('Initial number of question: {}'.format(len(cur_idx)))

    cur_idx = apply_index_filter(cur_idx, questions['text'].iloc, is_valid_unicode_range,
                                 filter_name='Valid Unicode symbols')

    print('Lemmatizing questions text for applying further filters')
    question_lemmas = lemmatize_all(questions['text'])

    cur_idx = apply_index_filter(cur_idx, question_lemmas, lambda sent_tokens: obscene_filter(sent_tokens, True),
                                 filter_name='Obscene words')

    freq_df = pd.read_csv('freqrnc2011.csv', sep='\t')

    ipm = {}
    for lemma, ipm_val in freq_df[['Lemma', 'Freq(ipm)']].itertuples(index=False):
        ipm[lemma] = ipm_val + ipm.get(lemma, 0.)


    def ipm_filter(sent_lemmas: Sequence[str]) -> bool:
        # Non-cyrillic tokens are ignored and don't affect filtering
        for token in sent_lemmas:
            if is_valid_unicode_range(token, (0x0400, 0x04ff)) and ipm.get(token, float('inf')) < IPM_LOWER_THRESHOLD:
                return False
        return True


    cur_idx = apply_index_filter(cur_idx, question_lemmas, ipm_filter, filter_name='low IPM')

    # Length filtering
    questions['q_len'] = questions['text'].apply(lambda s: len(s.split()))
    # sns.set_theme(style="darkgrid")
    # fig = sns.displot(questions['q_len'])
    # fig.set_xlabels('Question length')
    # plt.show()

    cur_idx = apply_index_filter(cur_idx, questions['q_len'].iloc, lambda q_l: q_l >= MIN_LEN,
                                 filter_name='Too short sentence')
    cur_idx = apply_index_filter(cur_idx, questions['q_len'].iloc, lambda q_l: q_l <= MAX_LEN,
                                 filter_name='Too long sentence')

    # Construct resulting table
    res_df = questions.iloc[cur_idx]
    columns = list(res_df.columns)
    columns.remove('q_len')
    if not text_was_presented:
        # If questions text isn't presented in source table, the resulting table won't contain it too.
        columns.remove('text')

    file_dir = path.dirname(questions_data_path)
    res_df.to_csv(
        path.join(file_dir, 'filtered_questions.csv'),
        index=False,
        sep=';',
        columns=list(columns)
    )
