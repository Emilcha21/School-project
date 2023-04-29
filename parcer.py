from bs4 import BeautifulSoup
import requests
import random
import threading
import time
import sqlite3


class Parcer:

    def __init__ (self, max_threads = 1):
        self.max_threads = max_threads
        self.threads= []
        self.lxml = {}
        self.user_agent_list = [
                                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
                                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
                                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
                                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                                ]
    def get_lxml(self, URL, params = {}):
        """Осуществляет get запрос серверу с параметрами запроса params,
        в случае не получения ответа ждет пол секунды и повторяет запрос.
        В случае удачи кладет lxml код страницы в список lxml, из которого в последствии
        можно извлечь требуемую информацию"""
        try:
            self.lxml.update({URL : requests.get(URL, params = params, headers = {'User-Agent': random.choice(self.user_agent_list)}).text})
            if len(BeautifulSoup(self.lxml[URL], "lxml").find_all("div", class_="th-item")) == 0:
                print(self.lxml[URL])
        except Exception as ex:
            print(ex)
            self.get_lxml(URL, params)

    def wait(self, max_threads):
        """Ожидает пока количество активных потоков не станет меньше константы max_threads
        задаваемой при инициализации объекта"""
        while len(self.threads) > max_threads:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

    def parce(self, links):
        """Очищает результаты предудущего вызова данной функции и
        осуществляет параллельные запросы к страницам (параметры запросов передаются в списке links)
        После выполнения возвращает список lxml страниц
        """
        counter = 0
        self.lxml = {}
        for link in links:
            counter += 1
            print("Парсится ", counter, " страница из ", len(links))
            self.threads.append(threading.Thread(target = self.get_lxml, args = (link, )))
            self.threads[-1].start()
            self.wait(self.max_threads)
        self.wait(0)
        return self.lxml

def getData(myParce, pages):
    links = []
    for number in range(1, pages + 1):
        links.append("https://v.lordfilm.film/films/page/" + str(number) + "/")
    films = {}
    for lxml in myParce.parce(links).values():
        page = BeautifulSoup(lxml, "lxml")
        for film in page.find_all("div", class_="th-item"):
            films.update({film.find("a", class_="th-in with-mask").get('href'):
                            {'name': film.find("div", class_="th-title").text,
                            'ear': film.find("div", class_="th-series").text,
                            'opisanie': None}
                        })
    links = films.keys()
    for lxml in myParce.parce(links).items():
        films.get(lxml[0]).update({'opisanie': BeautifulSoup(lxml[1], "lxml").find("div", class_="fdesc clearfix slice-this").text})
    return films

def addBD(data):
    db = sqlite3.connect("KINO3.db")
    cur = db.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS KINO3 (
        ID INTEGER PRIMARY KEY,
        RESURS TEXT,
        NAME TEXT,
        GOD TEXT,
        OPISANIE TEXT,
        LINK_STR TEXT
    )""")
    db.commit()
    counter = 0
    for film in data.items():
        counter += 1
        if counter % 10 == 0:
            print("Загружено: ", counter, " фильмов")
        cur.execute("""INSERT INTO KINO3 (RESURS, NAME, GOD, OPISANIE, LINK_STR) VALUES (?, ?, ?, ?, ?);""", ("lordfilm", data.get(film[0]).get('name'), data.get(film[0]).get('ear'), data.get(film[0]).get('opisanie'), film[0]))
        db.commit()
    print("Загружено: ", counter, " фильмов")

addBD(getData(Parcer(1), 100))
