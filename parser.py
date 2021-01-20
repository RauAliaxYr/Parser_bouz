import csv
import datetime
import logging
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from bs4 import BeautifulSoup
from pycbrf.toolbox import ExchangeRates


class Parser:
    URLS = []
    HEADERS = {}
    PROXIES = {}

    # Конечный каталог с товарами
    CATALOG = []

    e_mail_from = ''
    password = ''
    e_mail_to = ''

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
    for url in parser.URLS:
        html = get_html(url, parser)
        if html.status_code == 200:
            count_of_pages = search_count_of_page(html.text)
            for i in range(0, count_of_pages + 1):
                print(f'Cтраница {i} из {count_of_pages}')
                html = get_html(url + f'?PAGEN_1={i}', parser)
                if html.status_code == 200:
                    parser.CATALOG.append(get_content(html.text))
                else:
                    logging.warning(f"Статус код {html.status_code}")
        else:
            logging.warning(f"Статус код {html.status_code}")


def get_html(url, parser):
    if parser.PROXIES:

        try:
            req = requests.get(url, proxies=parser.PROXIES)
            logging.info("Подключились к прокси")
        except Exception:
            logging.warning("Не достучались до прокси")
    else:
        logging.info("Запрос без прокси")
        req = requests.get(url, headers=parser.HEADERS)
    return req


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


def get_content(html):
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
            obj = create_obj(name, article, cost, link)
            data.append(obj)
        except Exception:
            logging.warning(f"Не получилось спарсить товар {name} ")
    return data


def create_obj(name, article, cost, link):
    obj = []
    obj.append(name)
    obj.append(article)
    if cost != "0 руб":
        obj.append(cost + ' руб')
        rates = ExchangeRates(datetime.date.today())
        curse = rates['USD'].value
        dolares = round(float(cost.replace(' ', '')) / float(curse), 2)
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


def parse_controller():
    time_to_parse = datetime.date.today()
    now = True

    while True:
        if time_to_parse < datetime.date.today():
            now = True
        if now:
            now = False
            p = Parser
            p.__init__(p)
            parse_start(p)
            time_to_parse = datetime.date.today() + datetime.timedelta(days=1)


parse_controller()
