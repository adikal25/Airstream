from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args= {
    'owner': 'kalidindi',
    'start_date': datetime(2025,6 , 23, 0, 0, 0),
}



def get_data():
    import requests
    import json
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res


def format_data(res):
    import requests
    import json
    data={}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    
    data['gender'] = res['gender']
    data['address'] = (
        str(res['location']['street']['number']) + ' ' + res['location']['street']['name'] + ' ' +
        res['location']['city'] + ' ' +
        res['location']['state'] + ' ' +
        res['location']['country']
    )
    data['postcode'] = res['location']['postcode']
    data['email '] = res['email']
    data['username']= res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data
    

def stream_data():
    import json
    import requests
    from kafka import KafkaProducer
    import time
    # data = json.dumps(format_data(get_data()), indent=4)
    # return data
    
    data = get_data()
    res=format_data(data)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=2500)
    
    producer.send('user_data', json.dumps(res).encode('utf-8'))


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task =PythonOperator(
        task_id='stream_data_from_api'
    )

   

    