"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

"""
Escriba el codigo que ejecute la accion solicitada.
"""
import pandas as pd
import zipfile
import os
# pylint: disable=import-outside-toplevel


input_dir = 'files/input/'
output_dir = 'files/output/'

#Esto no me está dando y no sé por qué. Tocó usar IA

def read_all_compressed_csvs(directory):
    df = pd.DataFrame()
    for zip_file in os.listdir(directory):
        if zip_file.endswith('.zip'):
            zip_path = os.path.join(directory, zip_file)
            with zipfile.ZipFile(zip_path, 'r') as z:
                for csv_file in z.namelist():
                    if csv_file.endswith('.csv'):
                        with z.open(csv_file) as f:
                            df = pd.concat([df, pd.read_csv(f)], ignore_index=True)
    return df

# Function to clean and process client data
def process_client_data(df):
    df['job'] = df['job'].str.replace('.', '').str.replace('-', '_')
    df['education'] = df['education'].str.replace('.', '_').replace('unknown', pd.NA)
    df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    df['mortgage'] = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    return df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]

# Function to clean and process campaign data
def process_campaign_data(df):
    df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    df['last_contact_date'] = pd.to_datetime(df['day'].astype(str) + '-' + df['month'] + '-2022', format='%d-%b-%Y')
    return df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]

# Function to clean and process economics data
def process_economics_data(df):
    return df[['client_id', 'cons_price_idx', 'euribor_three_months']]

def clean_campaign_data():
    # Leer si se hacerlo
    all_data = read_all_compressed_csvs(input_dir)

    # Process data
    client_df = process_client_data(all_data)
    campaign_df = process_campaign_data(all_data)
    economics_df = process_economics_data(all_data)

    # Crear output. Al menos esto si lo sé hacer
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Salvar output terminado
    client_df.to_csv(output_dir + 'client.csv', index=False)
    campaign_df.to_csv(output_dir + 'campaign.csv', index=False)
    economics_df.to_csv(output_dir + 'economics.csv', index=False)
    
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return


if __name__ == "__main__":
    clean_campaign_data()


if __name__ == "__main__":
    clean_campaign_data()
