import pandas as pd

import argparse
from sys import stdin, stdout
from typing import Sequence, Tuple, Generator, Union

from utils.download import get_q_text


def is_broken(url: str, is_error: Sequence[Union[bool, int]]) -> float:
    real_text = get_q_text(url)
    if real_text is None:
        return 1.
    bad_n = sum(int(val) for val in is_error)
    return bad_n / len(is_error)


def propagate_verdict(df: pd.DataFrame) -> pd.DataFrame:
    mask = (df['ACCEPT:verdict'] == '-')
    if not mask.any():
        return df

    mask = mask & (~df['ACCEPT:comment'].isna())
    comments = df.loc[mask, 'ACCEPT:comment'].tolist()
    comment = '; '.join(comments)
    res = df.copy()
    res['ACCEPT:verdict'] = ['-'] * len(res)
    res['ACCEPT:comment'] = [comment] * len(res)
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Accepting user answers from Yandex Toloka')
    parser.add_argument(
        '-i',
        '--input',
        nargs='?',
        type=argparse.FileType('r', encoding='UTF-8'),
        help='Input file'
    )
    parser.add_argument(
        '-o',
        '--output',
        nargs='?',
        type=argparse.FileType('w', encoding='UTF-8'),
        help='Output file'
    )
    parser.add_argument(
        '--decline-only',
        action='store_true',
        help='Do not accept answers'
    )
    parser.add_argument(
        '--no-prop',
        action='store_true',
        help='Do not propagate answer rejection on whole task page'
    )
    parser.add_argument(
        '--undef-out',
        action='store_true',
        help='Output all lines, even if answer has been accepted neither rejected'
    )
    parser.add_argument(
        '-t',
        '--toloka',
        action='store_true',
        help='Convert output to format expected by Toloka'
    )
    args = parser.parse_args()

    if args.input is None:
        in_f = stdin
    else:
        in_f = args.input
    ans = pd.read_csv(in_f, sep='\t')

    # Choose ids with broken url
    broken_candidate = set(
        ans[ans['GOLDEN:class'].isna() & (ans['OUTPUT:q_1_error'] == True)]['INPUT:question_1_id']
    )
    broken_candidate.update(
        ans[ans['GOLDEN:class'].isna() & (ans['OUTPUT:q_2_error'] == True)]['INPUT:question_2_id']
    )

    surely_broken = set()
    unsurely_broken = set()
    for q_id in broken_candidate:
        q_url = None
        error_ans = ans[ans['INPUT:question_1_id'] == q_id]
        if q_url is None and len(error_ans) > 0:
            q_url = str(next(iter(error_ans['INPUT:question_1_url'])))
        is_error = error_ans['OUTPUT:q_1_error'].tolist()
        error_ans = ans[ans['INPUT:question_2_id'] == q_id]
        if q_url is None and len(error_ans) > 0:
            q_url = str(next(iter(error_ans['INPUT:question_2_url'])))
        is_error += error_ans['OUTPUT:q_2_error'].tolist()

        broken_confidence = is_broken(q_url, is_error)
        if broken_confidence >= 0.5:
            unsurely_broken.add(q_id)
            if broken_confidence >= .7:
                surely_broken.add(q_id)

    # Accept answers marked as error, if question may be inaccessible
    if not args.decline_only:
        ans.loc[
            ans['INPUT:question_1_id'].isin(unsurely_broken) & (ans['OUTPUT:q_1_error']), 'ACCEPT:verdict'
        ] = '+'
        ans.loc[
            ans['INPUT:question_2_id'].isin(unsurely_broken) & (ans['OUTPUT:q_2_error']), 'ACCEPT:verdict'
        ] = '+'
        ans.loc[
            ans['INPUT:question_1_id'].isin(unsurely_broken) & (ans['OUTPUT:q_1_error']), 'ACCEPT:comment'
        ] = 'Неправильная ссылка на 1 вопрос'
        ans.loc[
            ans['INPUT:question_2_id'].isin(unsurely_broken) & (ans['OUTPUT:q_2_error']), 'ACCEPT:comment'
        ] = 'Неправильная ссылка на 2 вопрос'

    # Decline answers marked as correct, if question is surely inaccessible
    mask = ans['INPUT:question_1_id'].isin(surely_broken) & ~(ans['OUTPUT:q_1_error'])
    ans.loc[mask, 'ACCEPT:verdict'] = '-'
    comments = ans.loc[mask, 'INPUT:question_1_url'].apply(
        lambda url: f'Вопрос №1, отмеченный как доступный, недоступен по ссылке ({url})')
    ans.loc[mask, 'ACCEPT:comment'] = comments

    mask = ans['INPUT:question_2_id'].isin(surely_broken) & ~(ans['OUTPUT:q_2_error'])
    ans.loc[mask, 'ACCEPT:verdict'] = '-'
    comments = ans.loc[mask, 'INPUT:question_2_url'].apply(
        lambda url: f'Вопрос №2, отмеченный как доступный, недоступен по ссылке ({url})')
    ans.loc[mask, 'ACCEPT:comment'] = comments

    # Decline answers marked as error, if question is surely accessible
    mask = ~ans['INPUT:question_1_id'].isin(unsurely_broken) & (ans['OUTPUT:q_1_error'])
    ans.loc[mask, 'ACCEPT:verdict'] = '-'
    comments = ans.loc[mask, 'INPUT:question_1_url'].apply(
        lambda url: f'Вопрос №1, отмеченный как недоступный, доступен по ссылке ({url})')
    ans.loc[mask, 'ACCEPT:comment'] = comments

    mask = ~ans['INPUT:question_2_id'].isin(unsurely_broken) & (ans['OUTPUT:q_2_error'])
    ans.loc[mask, 'ACCEPT:verdict'] = '-'
    comments = ans.loc[mask, 'INPUT:question_2_url'].apply(
        lambda url: f'Вопрос №2, отмеченный как недоступный, доступен по ссылке ({url})')
    ans.loc[mask, 'ACCEPT:comment'] = comments

    if not args.no_prop and 'ASSIGNMENT:assignment_id' in ans.columns:
        # Propagate declined answers on whole page
        grouped_df = ans.groupby('ASSIGNMENT:assignment_id')
        ans = grouped_df.apply(propagate_verdict).reset_index(drop=True)

    if not args.undef_out or args.toloka:
        # Remove lines with empty verdict
        ans = ans[~ans['ACCEPT:verdict'].isna()]
        if 'ASSIGNMENT:status' in ans.columns:
            ans = ans[ans['ASSIGNMENT:status'] == 'SUBMITTED']

    if args.toloka:
        # Convert to correct Toloka decision format
        ans = ans[['ASSIGNMENT:assignment_id', 'ACCEPT:verdict', 'ACCEPT:comment']]

    if args.output is None:
        out_f = stdout
    else:
        out_f = args.output

    if len(ans) == 0:
        pass
    else:
        ans.to_csv(out_f, sep='\t', header=True, index=False)
