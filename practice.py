from prefect import task, Flow

@task
def say_hello():
    print('Hello!')


with Flow('My first Flow') as flow:
    say_hello()

flow.run()
