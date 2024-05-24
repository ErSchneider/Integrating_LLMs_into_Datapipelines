import apache_beam as beam
import sqlite3
import requests as r
import json
from litellm import completion
import os
import datetime

def get_data(_):

    res = r.get(f"https://newsdata.io/api/1/news?apikey={os.environ.get('newsdata_key')}&q=openai")
    assert res.status_code == 200, f'server responded with status code {res.status_code}'

    return [res.text]


def summarize(content):
    messages = [{
        "content": '''
        The following JSON contains various articles in different languages related to OpenAI for the same date.
        Based on the contents of the JSON: Answer the question 'What is currently happening related to OpenAI?' in a few sentences. 
        Reply with only the summarization and the date of the articles in the following format: {"date": date, "summarization": summarization}.
        Do not mention the articles.
                    JSON: ''' + content,
        "role": "user"
    }]

    response = completion(model="gpt-3.5-turbo", messages=messages)
    response_text = response.choices[0].message.content

    try:
        ret = json.loads(response_text)
    except:
        print(f'failed to convert the following response_text: {response_text}')
        return
    return ret

def write_to_db(entry):
    with sqlite3.connect('database.db') as connection:
        cursor = connection.cursor()
        try:
            data_values = [entry[x] for x in entry]
            cursor.execute('''
                INSERT INTO summarization_v0_output (date, summarization)
                VALUES (?, ?)
            ''', data_values)
        except:
            print(f'failed to insert: {entry}')
        connection.commit()
        cursor.close()

def run_pipeline():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Erstellung' >> beam.Create([None])
            | 'Extraktion' >> beam.ParDo(get_data)
            | 'Transformation' >> beam.Map(summarize)
            | 'Speicherung' >> beam.Map(write_to_db)
            #| 'PrintOutput' >> beam.Map(lambda x: print(x))
        )

if __name__ == '__main__':
    start = datetime.datetime.now()
    print(start)
    run_pipeline()
    end = datetime.datetime.now()
    print(end)
    print(end-start)