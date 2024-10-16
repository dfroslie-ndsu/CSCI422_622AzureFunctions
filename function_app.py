import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import pandas as pd
import os


from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Function to get the connection string from Azure Key Vault
def get_connection_string():
    key_vault_name = "access-data-kv"
    secret_name = "data-connectionstring"
    kv_uri = f"https://{key_vault_name}.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)

    secret = client.get_secret(secret_name)
    return secret.value


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger_test")
def http_trigger_test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.blob_trigger(arg_name="myblob", path="inputs/{name}.csv",
                  connection="afsession1data_STORAGE") 
def BlobTrigger_test(myblob: func.InputStream):

    blob_name = myblob.name
    file_name = os.path.basename(blob_name)
    logging.info(f"Processing CSV file: {blob_name}")
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(myblob)       
    logging.info(df.head())

    required_columns = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    if all(column in df.columns for column in required_columns):
        # Add a new column "Diff" which is the difference between "High" and "Low"
        df['Diff'] = df['High'] - df['Low']
        
        new_csv_data = df.to_csv(index=False)
        
        # Retrieve the connection string from environment variables
        connection_string = get_connection_string()

        # Upload the new CSV file back to the blob storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("outputs")
        blob_client = container_client.get_blob_client(file_name)
        blob_client.upload_blob(new_csv_data, overwrite=True)
        
    else:
        logging.error("CSV file does not contain the required columns.")
    
