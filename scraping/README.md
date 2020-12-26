# Scrapping

Python module which contains spiders for **Yandex Q** portal.

Module uses `scrapy` framework to manage scrapping process.

## Prerequisites

All necessary dependencies are listed in projects root.
You have to install following python packages:
* `scrapy==2.4`
* `scrapy-useragents==0.0.1`
* `sqlalchemy==1.3`

## Running spiders

To run **Yandex Q** spider execute following command from module root:

```shell
scrapy crawl yandex_questions
```
It's highly recommended to use scrapy jobs feature to be able to restart scrapping process:
```shell
scrapy crawl yandex_questions -s JOBDIR=crawls/yandex_questions
```
The previous spider collects questions' and answers' text.
Please, make sure that your actions don't violate
**Yandex [Term of Use](https://yandex.ru/legal/q_termsofuse/)**.

To run spider, which definitely does not violate user license, use following command:
```shell
scrapy crawl yandex_question_ids
```
This spider collects only valid question urls and corresponding tags.

More useful scrapy commands can be found [here](https://docs.scrapy.org/en/2.4/topics/commands.html).

## Saving to SQL Database
If you want to save parsed items to SQL database, modify `DB_SETTINGS` variable in [settings.py](./scraping/settings.py) file. 
`DB_SETTINGS['url']` must contain correct _sqlalchemy_ engine url.
`DB_SETTINGS['connect_args']` is an optional dict passed to `create_engine()` function.

Example of `DB_SETTINGS` variable for **PostgreSQL** connection via SSL:
```python
DB_SETTINGS = {
    'url': 'postgresql+psycopg2://username:password@host:5432/db_name',
    'connect_args': {
        "sslmode": "verify-ca",
        "sslcert": './client-cert.pem',
        "sslkey": './client-key.pem',
        "sslrootcert": './server-ca.pem',
    }
}
```
More details about possible formats can be found in _sqlalchemy_ [documentation](https://docs.sqlalchemy.org/en/13/core/engines.html).

By default, parsed question are saved into **SQLite3** database file.
