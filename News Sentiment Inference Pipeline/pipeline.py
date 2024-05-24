import apache_beam as beam
import sqlite3
import pickle
import joblib
import requests as r
import json
from litellm import completion
import os
import datetime

VECTORIZER_CACHE = joblib.Memory(location='./cache', verbose=0)
CLF_CACHE = joblib.Memory(location='./cache', verbose=0)


@VECTORIZER_CACHE.cache
def load_vectorizer():
    with open('vectorizer.pkl', 'rb') as f:
        vectorizer = pickle.load(f)
    return vectorizer

@CLF_CACHE.cache
def load_CLF():
    with open('clf_balanced_fitted_optimized.pkl', 'rb') as f:
        clf = pickle.load(f)
    return clf


def get_data(_):

    res = r.get(f"https://api.nytimes.com/svc/topstories/v2/world.json?api-key={os.environ.get('nyt_key')}")

    assert res.status_code == 200, f'server responded with status code {res.status_code}'
    assert res.json()['status'] == 'OK', f'requests status: {res.json()["status"]}'

    return res.json()['results']

def prep_data(sample):
    preview = sample['title'] + ' ' + sample['abstract']
    vectorizer = load_vectorizer()
    return (preview, vectorizer.transform([preview]))


def get_prediction(input_tuple):
    clf = load_CLF()
    preview, vectorized_preview = input_tuple

    return {'preview':preview, 'label':clf.predict(vectorized_preview)[0]}

def get_LLM_prediction(raw_sample):
    messages = [{
        "content": '''Extract headline and abstract from the following article. Perform sentiment analysis based on headline and abstract, and label it with positive, neutral or negative sentiment. 
                    Respond with a JSON in the following format: {"preview": "headline + article", "label": "pos", "neu" or "neg"}. 
                    Ensure that all special characters that could break the JSON are properly escaped. 
                    Article-JSON: ''' + json.dumps(raw_sample),
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
                INSERT INTO inference_v0_output (preview, label)
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
            | 'Vorbereitung' >> beam.Map(prep_data)
            | 'Inferenz' >> beam.Map(get_prediction)
            #| 'Inferenz' >> beam.Map(get_LLM_prediction)
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