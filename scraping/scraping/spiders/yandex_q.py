from scrapy import Spider, Request
from scrapy.http import Response

import re
from typing import Optional, Generator
from random import shuffle

from scraping.items import Question, Answer


class YandexQuestionsSpider(Spider):
    name = 'yandex_questions'
    allowed_domains = ['yandex.ru', 'yandex.com', 'yandex.by', 'yandex.kz']
    url_pattern = re.compile(r'(((https?:)?//)?yandex.\w+)?(?P<rel_url>/q/(?P<type>question|tag|user|profile|org|loves|rating+)/(?P<thread>[a-zA-Z]+/)?(?P<id>[^/]+)/(?P<suffix>[^\?]*))')
    not_found_texts = [
        'Кажется, этой страницы не\xa0существует',
        'Кажется, этой страницы не существует'
    ]

    def start_requests(self):
        start_urls = [
            'https://yandex.ru/q/',
            'https://yandex.ru/q/themes/',
            'https://yandex.ru/q/rating/all/',
            'https://yandex.ru/q/loves/'
        ]
        for url in start_urls:
            yield Request(
                url,
                priority=1,
                callback=self.follow_urls,
                dont_filter=True
            )

    def follow_urls(self, response: Response, parent_id: Optional[str] = None) -> Generator[Request, None, None]:

        # Get all urls from page and filter relevant
        urls = map(self.url_pattern.match, set(response.xpath('//*[@href]/@href').getall()))
        urls = list(filter(lambda m: m is not None, urls))
        if len(urls) == 0:
            self.logger.info('Page %s has no valid urls to follow', response.url)
        shuffle(urls)

        for match_res in urls:
            rel_url = match_res.group('rel_url')
            url_type = match_res.group('type')
            if url_type == 'question':
                next_q_id = match_res.group('id')
                yield Request(
                    response.urljoin(rel_url),
                    priority=2,
                    callback=self.parse,
                    cb_kwargs={
                        'q_id': next_q_id,
                        'parent_id': parent_id
                    })
            elif url_type in ('user', 'profile', 'tag', 'loves', 'rating', 'org'):
                yield Request(
                    response.urljoin(rel_url),
                    priority=1,
                    callback=self.follow_urls,
                    meta={'filter_mode': 'session'}
                )

    def parse(self, response: Response, q_id: str, parent_id: Optional[str] = None, **kwargs):
        non_found_page_cnt = 0
        questions = set()
        for q in response.xpath('//h1/text()').getall():
            if q in self.not_found_texts:
                non_found_page_cnt += 1
            else:
                questions.add(q)
        questions = list(questions)

        if len(questions) == 0 and non_found_page_cnt == 0:
            self.logger.error('No h1 header found on %s page', response.url)
        elif len(questions) > 0:
            if len(questions) > 1:
                self.logger.warn('Page on %s contains more than one header. The question may be parsed incorrectly',
                                 response.url)

            question = questions[0]
            # Get answers
            answers = []
            for block in response.xpath('//div[@id="page"]/div/div[2]/section/div[2]/div[1]/div[@data-id]'):
                text = block.xpath('div[2]//div[@class="formatted"]//text()').getall()
                text = '\n'.join(text)
                if text == '':
                    # Sometimes text is empty because answer block contains "" button and structure is different
                    # Decided to skip such truncated answer
                    continue

                # Parse answer rating
                rating_plus_button = block.xpath('.//button[text()[contains(.,"ороший ответ")]]')

                pluses = rating_plus_button.xpath('string(span[1])').get()
                pluses = ''.join(filter(lambda c: c.isalnum(), pluses))
                try:
                    pluses = 0 if pluses == '' else int(pluses)
                except ValueError:
                    pluses = None

                minuses = rating_plus_button.xpath('string(following-sibling::button[1]/span[1])').get()
                minuses = ''.join(filter(lambda c: c.isalnum(), minuses))
                try:
                    minuses = 0 if minuses == '' else int(minuses)
                except ValueError:
                    minuses = None

                answers.append(Answer(text=text, pluses=pluses, minuses=minuses))

            # Get tags
            tags = response.xpath('//h1/following-sibling::div/div/a/text()').getall()
            yield Question(
                question=question,
                question_id=q_id,
                parent_id=parent_id,
                tags=tags,
                answers=answers,
                url=response.url
            )
        for req in self.follow_urls(response, q_id):
            yield req
