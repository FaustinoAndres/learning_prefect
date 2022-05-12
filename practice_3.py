from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import datetime
import requests
import json

@task(log_stdout=True, max_retries=3, retry_delay=datetime.timedelta(minutes=1))
def extract():
    print("*INFO: Vamos a pbtener la respuesta de la API")
    raw = requests.get('https://jsonplaceholder.typicode.com/post/1')
    print("*INFO: El código de respuesta de la API es {}".format(raw.status_code))
    raw = json.loads(raw.text)
    with open('data/raw.json', 'w', encoding='utf-8') as file:
        json.dumps(raw, file, ensure_ascii=False, indent=4)

    return raw

@task(log_stdout=True)
def transform(raw):
    print("*INFO: Ejecutando la transformación")
    transformed = raw['title']
    with open('data/transformed.json', 'w', encoding='utf-8') as file:
        json.dump(transformed, file, ensure_ascii=False, indent=4)
    return transformed

@task(log_stdout=True)
def load(transformed):
    print("*INFO: Vamos a procedes con la tarea load")
    print('Este es el título del primer objeto de la API de posts de JSONplaceholder')
    print(str(transformed))

schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

with Flow('Practice 2 - JSON Placeholder', schedule) as flow:
    raw = extract()
    transformed = transform(raw)
    load(transformed)

flow.run()
