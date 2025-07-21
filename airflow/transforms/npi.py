import requests
from google.cloud import bigquery

def npi_pull():
    url = 'https://npiregistry.cms.hhs.gov/api/?version=2.1&taxonomy_description=Neurology&city=Elk%20Grove%20Village&state=IL&limit=10'
    response = requests.get(url)
    json_data = response.json()
    records_to_load = json_data['results']
    
    project_id = "pipeline-466619"
    # 1. Instantiate the BigQuery client
    client = bigquery.Client(project=project_id)

    # 2. Set your project, dataset, and table IDs
    table_id = f"{project_id}.healthcare.npi"

    # 3. Configure the load job

    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Automatically infer the schema from the data
        # Use WRITE_TRUNCATE to overwrite the table if it exists
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # 4. Load the data from the list of dictionaries
    load_job = client.load_table_from_json(records_to_load, table_id, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    # 6. Print confirmation
    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows into table {table_id}.")