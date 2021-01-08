from unittest import TestCase
import pandas as pd

from sys import stderr

from utils.download import extend_dataframe


class Test(TestCase):
    def test_extend_dataframe(self):
        correct_df = pd.read_csv('test_q_ids.csv', sep=';')
        no_text_df = correct_df.copy(deep=True).drop(columns=['text'])

        result_df = extend_dataframe(no_text_df)
        if not correct_df.sort_index(axis=1).equals(result_df.sort_index(axis=1)):
            print('Different of downloaded and Ground Truth dataframe:', file=stderr)
            print(correct_df.sort_index(axis=1).compare(result_df.sort_index(axis=1)), file=stderr)
            self.fail()
