import apache_beam as beam
from bs4 import BeautifulSoup
import requests
import sqlite3
from litellm import completion
import json

def get_subsites(base_url):
    html_contents = []
    for i in range(1, 55):
        site = requests.get(f"{base_url}/catalogue/page-{i}.html")

        if site.status_code == 404:
            break
        else:
            site.encoding = 'utf-8'
            html_contents.append(site.text)

    return html_contents

def extract_book_info(html_content):
    book_info_list = []
    soup = BeautifulSoup(html_content, 'html.parser')
    books = soup.find_all('article', class_='product_pod')

    number_mapping = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5
    }

    for book in books:
        rating = book.select_one('p.star-rating')['class'][1]
        price = book.select_one('p.price_color').get_text().strip() 
        name = book.select_one('h3 a')['title']
        availability = book.select_one('p.availability').get_text().strip() 

        book_info = {
            'Rating': number_mapping[rating],
            'Price': price.replace('£', ''),
            'Name': name,
            'Availability': availability == 'In stock'
        }
        book_info_list.append(book_info)

    return book_info_list

def extract_book_info_LLM(html_content):
    messages = [{ "content": f'''Extract all book info from the following HTML. Create a list of JSON-Objects with the following schema for each book: Rating:Integer, Price:Float, Name:String, Availability:Boolean. HTML:{html_content}''',"role": "user"}]
    response = completion(model="gpt-3.5-turbo-16k", messages=messages)
    response_text = response.choices[0].message.content

    return json.loads(response_text)

def write_to_db(entry):
    with sqlite3.connect('../database.db') as connection:
        cursor = connection.cursor()

        data_values = [entry[x] for x in entry]
        
        cursor.execute('''
            INSERT INTO scraper_v0_output (rating, price, name, is_available)
            VALUES (?, ?, ?, ?)
        ''', data_values)

        connection.commit()
        cursor.close()

def run_pipeline():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Erstellung' >> beam.Create(['http://books.toscrape.com'])
            | 'Extraktion' >> beam.ParDo(get_subsites)
            | 'Transformation' >> beam.ParDo(extract_book_info)
            #| 'Transformation' >> beam.ParDo(extract_book_info_LLM)
            | 'Speicherung' >> beam.ParDo(write_to_db)
        )

if __name__ == '__main__':
    import datetime
    start = datetime.datetime.now()
    print(start)
    run_pipeline()
    end = datetime.datetime.now()
    print(end)
    print(end-start)