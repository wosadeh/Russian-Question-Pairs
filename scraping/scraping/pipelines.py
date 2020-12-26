# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from database import Base
from database.models import Answer, Question, Tag


class ScrapingPipeline:
    def process_item(self, item, spider):
        return item


class DatabaseSQLPipeline:
    """
    Pipeline which saves parsed question into SQL database
    using models from database module
    """
    def __init__(self, db_url: str, connect_args=None):
        self.db_url = db_url
        if connect_args is None:
            engine = create_engine(db_url)
        else:
            engine = create_engine(db_url, connect_args=connect_args)
        Base.metadata.create_all(engine, checkfirst=True)
        self.session_class = sessionmaker(bind=engine)

    @classmethod
    def from_crawler(cls, crawler):
        db_settings = crawler.settings.getdict("DB_SETTINGS")
        if not db_settings:  # if we don't define db config in settings
            raise KeyError('No DB_SETTINGS in crawler settings')  # then reaise error
        return cls(db_settings['url'], db_settings.get('connect_args', None))

    def open_spider(self, spider):
        self.session = self.session_class()

    def close_spider(self, spider):
        self.session.close()

    def process_item(self, item, spider):
        # Processed parsed tags
        tags = []
        new_tags_added = False
        for tag_name in item.get('tags', []):
            # Get tag object from DB or create if not exists
            try:
                tag_obj = self.session.query(Tag).filter(Tag.tag == tag_name).one()
            except NoResultFound:
                tag_obj = Tag(tag=tag_name)
                self.session.add(tag_obj)
                new_tags_added = True

            tags.append(tag_obj)
        # Add new tags to DB
        if new_tags_added:
            self.session.commit()

        # If same question is in DB, return
        question_short_name = item['question_id']
        if question_short_name is not None:
            q_obj = self.session.query(Question).filter(Question.short_name == question_short_name).one_or_none()
            if q_obj is not None:
                spider.logger.debug("The question with id='%s' is already exists", question_short_name)
                return item

        # Create new question objects
        question = Question(
            text=item['question'],
            short_name=question_short_name,
            parent_short_name=item.get('parent_id', None),
            url=item['url'],
        )
        question.tags.extend(tags)

        # Create DB object for each answer and attach them to question
        for answer_item in item.get('answers', []):
            text = answer_item['text']
            pluses = answer_item.get('pluses', None)
            minuses = answer_item.get('minuses', None)
            ans_obj = Answer(text=text, pluses=pluses, minuses=minuses)
            self.session.add(ans_obj)
            question.answers.append(ans_obj)
        # Commit all changes
        self.session.add(question)
        self.session.commit()
        return item
