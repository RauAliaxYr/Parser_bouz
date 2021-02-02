import asyncio
import csv
import datetime
import logging
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from time import time

import aiohttp as aiohttp
from bs4 import BeautifulSoup
from pycbrf.toolbox import ExchangeRates

logging.basicConfig(level=logging.INFO)


class Parser:
    URLS = []
    HEADERS = {}
    PROXIES = {}

    # Вспомогательные списки
    ALL_URLS = []
    HTMLS = {}
    all_HTMLS = []
    CATALOG = []

    e_mail_from = ''
    password = ''
    e_mail_to = ''

    curse = 0

    def __init__(self):
        # Инициализируем HEADER
        self.HEADERS = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/84.0.4147.105 Safari/537.36',
            'accept': '*/*'}

        # Проверяем, есть ли прокси
        with open("proxies.txt", 'r') as file:
            for line in file:
                proxies = {'http': line}
                self.PROXIES.update(proxies)

        # Инициализируем URLs для парсинга
        self.URLS.append("https://bouz.ru/catalog/kommutatory/")
        self.URLS.append("https://bouz.ru/catalog/marshrutizatory/")
        self.URLS.append("https://bouz.ru/catalog/opticheskie_transivery/")
        self.URLS.append("https://bouz.ru/catalog/servery/")
        self.URLS.append("https://bouz.ru/catalog/wifi/")
        self.URLS.append("https://bouz.ru/catalog/sistemy_konferentssvyazi/")
        self.URLS.append("https://bouz.ru/catalog/telefoniya_voip/")
        self.URLS.append("https://bouz.ru/catalog/komplektuyushchie_i_raskhodnye_materialy/")

        # Добавляем e-mail
        with open("e-mail.txt", 'r') as file:
            self.e_mail_from = file.readline()
            self.password = file.readline()
            self.e_mail_to = file.readline()



        # Актуальный курс валют
        rates = ExchangeRates(datetime.date.today())
        self.curse = rates['USD'].value

        logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

        logging.info("Парсер проинициализирован")


def parse_start(parser):
    logging.info("Начинаем парсить")
    parse(parser)
    logging.info("Парсить закончили")
    logging.info("Создаём CSV файл")
    creat_csv(parser.CATALOG)
    send_to_email(parser)


def parse(parser):
    asyncio.run(url_handler(parser))

    for urls, datas in parser.HTMLS.items():
        num = search_count_of_page(datas)
        print(num)
        for i in range(2, num + 1):
            parser.ALL_URLS.append(create_urls(i, urls))

    asyncio.run(page_handler(parser))

    asyncio.run(content_handler(parser))


async def url_handler(parser):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in parser.URLS:
            task = asyncio.create_task(get_html(url, parser, session))
            tasks.append(task)

        await asyncio.gather(*tasks)


async def page_handler(parser):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in parser.ALL_URLS:
            task = asyncio.create_task(get_html(url, parser, session))
            tasks.append(task)

        await asyncio.gather(*tasks)


async def content_handler(parser):
    tasks = []
    for url, html in parser.HTMLS.items():
        task = asyncio.create_task(get_content(html, parser))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def get_html(url, parser, session):
    if parser.PROXIES:
        try:
            async with session.get(url, proxies=parser.PROXIES) as res:
                data = await res.read()
                try:
                    dict = {url: data}
                    parser.HTMLS.update(dict)
                except Exception:
                    logging.warning("Не достучались до прокси")
            logging.info("Подключились к прокси")
        except Exception:
            logging.warning("Не достучались до прокси")
    else:
        try:
            async with session.get(url) as res:
                data = await res.read()
                try:
                    dict = {url: data}
                    parser.HTMLS.update(dict)
                except Exception:
                    logging.warning("Не достучались до url")
            logging.info("Подключились url")
        except Exception:
            logging.warning("Не достучались url")


def search_count_of_page(html):
    count_of_page = 0
    soup = BeautifulSoup(html, 'html.parser')
    block = soup.find('div', class_='module-pagination')
    pages = block.find_all('a', class_='dark_link')
    for page in pages:
        page_to_int = int(page.text)
        if page_to_int > count_of_page:
            count_of_page = page_to_int
    return count_of_page


def create_urls(num, url):
    return url + f'?PAGEN_1={num}'


async def get_content(html, parser):
    data = []
    soup = BeautifulSoup(html, 'html.parser')
    block = soup.find('div', class_='catalog_block items block_list')
    items = block.find_all('div', class_='item_block col-4 col-md-3 col-sm-6 col-xs-6')
    for item in items:
        try:
            title = item.find('div', class_='item-title')
            name = title.find('span').text
            article = item.find('span', class_='articul').text
            cost = item.find('span', class_='price_value').text
            link = item.find('a', class_='thumb shine').get('href')
            obj = create_obj(name, article, cost, link, parser)
            data.append(obj)
        except Exception:
            logging.info(f"Не получилось спарсить товар  ")
    parser.CATALOG.append(data)


def create_obj(name, article, cost, link, parser):
    obj = []
    obj.append(name)
    obj.append(article)
    if cost != "0 руб":
        obj.append(cost + ' руб')
        dolares = round(float(cost.replace(' ', '')) / float(parser.curse), 2)
        obj.append(str(dolares) + ' $')
    else:
        obj.append("Нет в наличии")

    obj.append('https://bouz.ru' + link)

    return obj


def creat_csv(catalog):
    FILE_NAME = 'catalog.csv'
    try:
        with open(FILE_NAME, 'w', newline='', encoding="utf-16")as file:
            write = csv.writer(file)
            for items in catalog:
                for item in items:
                    write.writerow(item)
        logging.info("CSV файл создан!")
    except Exception:
        logging.error("Ошибка записи в файл")


def send_to_email(parser):
    if not parser.e_mail_from or not parser.e_mail_to:
        logging.warning("Не найден email")
    else:
        message = MIMEMultipart()
        message["From"] = parser.e_mail_from
        message["To"] = parser.e_mail_to
        message["Subject"] = f"Catalog bouz.ru. Data: {datetime.date.today()}"
        message["Bcc"] = parser.e_mail_to
        message.attach(MIMEText("", "plain"))

        with open("catalog.csv", 'rb') as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= catalog.csv",
        )
        message.attach(part)
        text = message.as_string()

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context)as server:
            server.login(parser.e_mail_from, parser.password)
            server.sendmail(parser.e_mail_from, parser.e_mail_to, text)
            logging.info("CSV файл отправлен")


if __name__ == '__main__':
    t0 = time()
    p = Parser
    p.__init__(p)
    parse_start(p)
    print(time() - t0)
    del p


