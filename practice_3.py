from prefect import task, Flow
import datetime
import requests

@task(log_stdout=True, max_retries=3, retry_delay=datetime.timedelta(minutes=1))
def extract():
    print("*INFO: Vamos a pbtener la respuesta de la API")
    raw = requests.get('https://jsonplaceholder.typicode.com/post/1')
    print("*INFO: El código de respuesta de la API es {}".format(raw.status_code))
    raw = raw.json()
    return raw

@task(log_stdout=True)
def transform(raw):
    print("*INFO: Ejecutando la transformación")
    transformed = raw['title']
    return transformed

@task(log_stdout=True)
def load(transformed):
    print("*INFO: Vamos a procedes con la tarea load")
    print('Este es el título del primer objeto de la API de posts de JSONplaceholder')
    print(str(transformed))


with Flow('Practice 2 - JSON Placeholder') as flow:
    raw = extract()
    transformed = transform(raw)
    load(transformed)

flow.run()
