from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago ## deprecated in airflow 2.5.0
from datetime import datetime
import json


with DAG(
    dag_id="nasa_apod_postgres",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
) as dag:

    ## step 1 : create the table if not exists

    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")  # connection id defined in airflow connections for postgres database
        
        # sql query to create the table if it does not exist    
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title varchar(255),
            explanation TEXT,
            url TEXT,
            date DATE ,
            media_type varchar(50) 
        );
        """

        #Execute the query to create the table
        postgres_hook.run(create_table_query)
        return create_table_query

    ## step 2: extact the nasa api data - (astronomy picture of the day) [Extract pipeline]
    # https://api.nasa.gov/planetary/apod/?api_key=VZESAZdp1HDqmUA34gKXaidjAogsnp6rjeNtgTAT
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api", # connection id defined in airflow connections for nasa api
        endpoint="wp-json/wp/v2/apod-basic/",  # nasa api endpoint for astronomy picture of the day
        method="GET",
        data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},  # use the api key from the connection extra field
        response_filter=lambda response:response.json(), # convert the response to json format
        # log_response=True,
    )


    ## Step 3: TRANCEFORM the data - (convert the json data to a python dictionary) [Transform pipeline] (PICK the data we want to store in the database)

    # @task
    # def transform_apod_data(response):
    #     # extract the relevant fields from the response
    #     apod_data = {
    #         "title": response.get("title", ''),
    #         "explanation": response.get("explanation", ''),
    #         "url": response.get("url", ''),
    #         "date": response.get("date", ''),
    #         "media_type": response.get("media_type", '')
    #     }
    #     return apod_data
    @task
    def transform_apod_data(api_response):

        # Agar API single dictionary return kare,
        # to use list mein convert kar do
        if isinstance(api_response, dict):
            api_response = [api_response]

        if not api_response:
            raise ValueError("NASA API returned no records")

        transformed_data = []

        for item in api_response:
            media_type = item.get("media_type", "image")

            if media_type == "image":
                image_url = item.get("hdurl") or item.get("url")
            else:
                image_url = item.get("url")

            transformed_data.append({
                "title": item.get("title"),
                "explanation": item.get("explanation"),
                "url": image_url,
                "date": item.get("date"),
                "media_type": media_type,
            })

        return transformed_data



    ## Step4 : load the data to the postgres database - (insert the data into the table) [Load pipeline]

    # @task
    # def load_apod_to_postgres(apod_data):
    #     postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        
    #     # sql query to insert the data into the table
    #     insert_query = """
    #     INSERT INTO apod_data (title, explanation, url, date, media_type)
    #     VALUES (%s, %s, %s, %s, %s);
    #     """
        
    #     # Execute the query to insert the data
    #     postgres_hook.run(insert_query, parameters=(apod_data["title"], apod_data["explanation"], apod_data["url"], apod_data["date"], apod_data["media_type"]))
    #     return insert_query
    @task
    def load_apod_to_postgres(apod_data):
        postgres_hook = PostgresHook(
            postgres_conn_id="my_postgres_connection"
        )

        insert_query = """
            INSERT INTO apod_data
            (title, explanation, url, date, media_type)
            VALUES (%s, %s, %s, %s, %s);
        """

        for item in apod_data:
            postgres_hook.run(
                insert_query,
                parameters=(
                    item["title"],
                    item["explanation"],
                    item["url"],
                    item["date"],
                    item["media_type"],
                )
            )

        return f"Inserted {len(apod_data)} records into apod_data"


    ## step 5: verify the data with dbviwer 

    ## step 6 : define the task dependencies
    ##extract
    create_table() >> extract_apod ## ensure the table is created before extracting the data
    api_response = extract_apod.output
    ##transform
    transformed_data = transform_apod_data(api_response)
    ##load
    load_apod_to_postgres(transformed_data)
