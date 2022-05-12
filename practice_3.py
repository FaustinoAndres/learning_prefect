from prefect import task, Flow

import requests

@task(log_stdout=True)
def extract():
    print("*INFO: Vamos a pbtener la respuesta de la API")
    response = requests.get('https://jsonplaceholder.typicode.com/post')
    print("*INFO: El código de respuesta de la API es {}".format(response.status_code))
    response = response.json()
    return response

@task(log_stdout=True)
def load(response):
    output = response[0]['title']
    print('Este es el título del primer objeto de la API de posts de JSONplaceholder')
    print(str(output))


with Flow('Practice 2 - JSON Placeholder') as flow:
    raw = extract()
    load(raw)

flow.run()
