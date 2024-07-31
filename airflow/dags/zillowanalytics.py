from airflow import DAG
from datetime import timedelta, datetime
import json
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests


#Load JSON Config File

with open('/home/ubuntu/airflow/config_api.json','r') as config_file:
    api_host_key = json.load(config_file)


now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")


#Python Callable function used in DAG
def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']
    #return headers
    reponse = requests.get(url,headers = headers, params = querystring)
    reponse_data = reponse.json()

    #Specify the ouput file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.json'

    #Write the json response to a file
    with open(output_file_path, "w") as output_file :
        json.dump(reponse_data,output_file,indent = 4) #indent for pretty formatting 
    output_list = [output_file_path,file_str]
    return output_list



default_args = {
    'owner' : 'Yash-First-Airflow-DAG',
    'depends_on_past' : False,
    'start_date' : datetime(2024,7,30),
    'email':['yashvij1996@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(seconds = 15)
}



with DAG('zillow-analytics-dag',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        extract_zillow_data_var = PythonOperator(
            task_id = 'tsk_extract_zillow_data_var',
            python_callable = extract_zillow_data,
            op_kwargs={'url':'https://zillow56.p.rapidapi.com/search', 'querystring':{"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"}, 'headers':api_host_key , 'date_string':dt_now_string}
        )


        load_to_s3 = BashOperator(
            task_id = "task_load_to_S3",
            bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_zillow_data_var")[0]   }} s3://zillow-analytics-youtube/'
        )

        extract_zillow_data_var >> load_to_s3



