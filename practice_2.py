from prefect import task, Flow

import requests

@task
def extract():
    response = requests.get('https://jsonplaceholder.typicode.com/post')
    response = response.json()
    return response

@task
def load(response):
    output = response[0]['title']
    print('Este es el título del primer objeto de la API de posts de JSONplaceholder')
    print(str(output))


with Flow('Practice 2 - JSON Placeholder') as flow:
    raw = extract()
    load(raw)

flow.run()
