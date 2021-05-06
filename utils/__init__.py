from typing import Callable


class QuestionPairBase(object):
    """
    Class for question pairs
    Two pairs are equal, if they have small Levenshtein distance
    """
    def __init__(self,
                 left_id: int, left_text: str,
                 right_id: int, right_text: str,
                 similarity: float):
        self.left_id = left_id
        self.left_text = left_text
        self.right_id = right_id
        self.right_text = right_text
        self.similarity = similarity

    @staticmethod
    def _question_eq(left: str, right: str) -> bool:
        return left == right

    def __eq__(self, other) -> bool:
        left_left_eq = self._question_eq(self.left_text, other.left_text)
        right_right_eq = self._question_eq(self.right_text, other.right_text)

        if left_left_eq and right_right_eq:
            return True

        left_right_eq = self._question_eq(self.left_text, other.right_text)
        right_left_eq = self._question_eq(self.right_text, other.left_text)
        if left_right_eq and right_left_eq:
            return True
        else:
            return False

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        # order agnostic hash
        return self.left_text.__hash__() ^ self.right_text.__hash__()


def create_heuristic_comparator(eq_heuristic: Callable[[str, str], bool]):
    class QuestionPair(QuestionPairBase):
        @staticmethod
        def _question_eq(left: str, right: str) -> bool:
            return eq_heuristic(left, right)

    return QuestionPair
