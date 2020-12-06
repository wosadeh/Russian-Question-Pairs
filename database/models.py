from sqlalchemy import Column, Integer, UnicodeText, Unicode, Table, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from . import Base


class Answer(Base):
    __tablename__ = 'answers'

    id = Column(Integer, primary_key=True)
    text = Column(UnicodeText)
    pluses = Column(Integer, nullable=True)
    minuses = Column(Integer, nullable=True)
    question_id = Column(Integer, ForeignKey('questions.id'))
    question = relationship("Question", back_populates="answers")


class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    tag = Column(Unicode, nullable=False, unique=True)


QuestionTag = Table('question_tag',
                    Base.metadata,
                    Column('question_id', Integer, ForeignKey('questions.id')),
                    Column('tag_id', Integer, ForeignKey('tags.id')),
                    )


class Question(Base):
    __tablename__ = 'questions'

    id = Column(Integer, primary_key=True)
    text = Column(UnicodeText, nullable=False)
    url = Column(Unicode, nullable=False)
    short_name = Column(Unicode, nullable=True, unique=True)
    parent_short_name = Column(Unicode, nullable=True)
    tags = relationship('Tag', secondary=QuestionTag, backref="related_questions")
    answers = relationship('Answer', back_populates="question", cascade="all, delete")


class ParaPhrase(Base):
    __tablename__ = 'paraphrases'

    id = Column(Integer, primary_key=True)
    left_id = Column(Integer, ForeignKey('questions.id'), nullable=False)
    right_id = Column(Integer, ForeignKey('questions.id'), nullable=False)
    left = relationship(Question, uselist=False, foreign_keys=[left_id])
    right = relationship(Question, uselist=False, foreign_keys=[right_id])
    is_paraphrase = Column(Boolean)
