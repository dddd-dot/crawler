# -*- coding: utf-8 -*-

import scrapy
from pyquery import PyQuery as pq
from scrapy import Request

from movie.items import MovieItem


class DianyingSpider(scrapy.Spider):
    name = 'dianying'
    allowed_domains = ['www.dy2018.com']
    start_urls = ['http://www.dy2018.com/']
    start_page = 1

    def start_requests(self):
        yield self.make_requests_from_url('http://www.dy2018.com/html/gndy/dyzz/index.html')

    def parse(self, response):
        domain = 'http://www.dy2018.com'
        doc = pq(response.text)
        items = doc('.bd3 .co_content8 .ulink').items()
        for item in items:
            yield Request(domain + item.attr('href'), callback=self.parse_detail_page)
        self.start_page = self.start_page + 1
        yield self.make_requests_from_url(self.get_next_page_url(self.start_page))

    def parse_detail_page(self, response):
        doc = pq(response.text)
        movie = MovieItem()
        movie['movie_name'] = doc(
            '#header > div > div.bd2 > div.bd3 > div.co_area2 > div.title_all > h1').text().strip()
        movie['raw_url'] = response.url
        movie['cover_image'] = doc('#Zoom > p:nth-child(1) > img').attr('src')
        download_url = self.get_download_url(doc)
        movie['download_url'] = [url for url in download_url if url]
        yield movie

    def get_next_page_url(self, page):
        url = 'http://www.dy2018.com/html/gndy/dyzz/index_{page}.html'
        return url.format(page=page)

    def save_to_file(self, content):
        with open('movie.txt', 'a+') as f:
            f.write(content + '\n')

    def get_download_url(self, doc):
        download_url = []
        for i in doc('#Zoom table a').items():
            text = i.text()
            href = i.attr('href')
            thunder_href = i.attr('thunderhref')
            if text:
                download_url.append(href)
            if href and href != '#':
                download_url.append(text)
            if thunder_href and thunder_href != '#':
                download_url.append(thunder_href)
        return list(set(download_url))
