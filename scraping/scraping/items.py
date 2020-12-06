# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Question(scrapy.Item):
    question = scrapy.Field()
    question_id = scrapy.Field()
    parent_id = scrapy.Field()
    tags = scrapy.Field()
    answers = scrapy.Field()
    url = scrapy.Field()


class Answer(scrapy.Item):
    text = scrapy.Field()
    pluses = scrapy.Field()
    minuses = scrapy.Field()
