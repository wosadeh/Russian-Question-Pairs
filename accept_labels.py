import pandas as pd
import numpy as np

import argparse
from datetime import datetime, timedelta
from warnings import warn
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


def last_ctrl_good_ts(df: pd.DataFrame, time_col: str, ctrl_threshold=1.):
    windows_size = 40
    last_ts = datetime.today() + timedelta(days=366)
    if len(df) < windows_size:
        ctrl_df = df[~df['GOLDEN:class'].isna()]
        ctrl_frac = (ctrl_df['GOLDEN:class'] == ctrl_df['OUTPUT:class']).astype(float).mean()

        # Special rules for workers with low number of control answers
        if len(ctrl_df) <= 2:
            loc_thresh = 0.49
        elif len(ctrl_df) <= 4:
            loc_thresh = 0.5
        else:
            loc_thresh = ctrl_threshold

        if ctrl_frac < loc_thresh:
            last_ts = datetime.fromtimestamp(0)
        return last_ts

    df = df.sort_values([time_col, 'ASSIGNMENT:assignment_id'], ascending=False).reset_index(drop=True)
    # 1 question from 5 is control
    for j in range(windows_size, len(df) + 1, 5):
        ctrl_df = df.loc[j - windows_size:j]
        ctrl_df = ctrl_df[~ctrl_df['GOLDEN:class'].isna()]
        ctrl_frac = (ctrl_df['GOLDEN:class'] == ctrl_df['OUTPUT:class']).astype(float).mean()
        if ctrl_frac < ctrl_threshold:
            last_ts = df.at[j, time_col]
            break

    return last_ts


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
        '--ctrl',
        nargs='?',
        type=float,
        default=argparse.SUPPRESS,
        help='Decline for bad answers on control questions in sliding window'
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

    if 'ASSIGNMENT:submitted' in ans.columns:
        ans['ASSIGNMENT:submitted'] = ans['ASSIGNMENT:submitted'].apply(lambda s: datetime.fromisoformat(s))
    if 'ASSIGNMENT:started' in ans.columns:
        ans['ASSIGNMENT:started'] = ans['ASSIGNMENT:started'].apply(lambda s: datetime.fromisoformat(s))

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

    if 'ASSIGNMENT:submitted' in ans.columns:
        time_col = 'ASSIGNMENT:submitted'
    elif 'ASSIGNMENT:started' in ans.columns:
        time_col = 'ASSIGNMENT:started'
    else:
        time_col = None

    if 'ctrl' in args and time_col is not None:
        if args.ctrl is None:
            ctrl = 0.75
        else:
            ctrl = args.ctrl
        if 1. < ctrl <= 100:
            ctrl /= 100
        if 0. <= ctrl <= 1.:
            grouped = ans.groupby('ASSIGNMENT:worker_id')
            # Calculate the latest time point when control answers were nice
            last_ts = grouped.apply(lambda df: last_ctrl_good_ts(df, time_col, ctrl)).reset_index().rename(columns={0: 'last_ts'})
            ans = pd.merge(ans, last_ts, left_on='ASSIGNMENT:worker_id', right_on='ASSIGNMENT:worker_id', how='left')

            ans.loc[ans[time_col] >= ans['last_ts'], 'ACCEPT:verdict'] = '-'
            ans.loc[ans[time_col] >= ans['last_ts'], 'ACCEPT:comment'] = 'Неправильные ответы на контрольные вопросы.'
            ans = ans.drop(columns='last_ts')
        else:
            warn('Incorrect "ctrl" argument value. No rules based on control questions will be applied')

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
