import pandas as pd 
import datetime as dt 
from google.cloud import storage
import os 

# Extract the data from soucre
def extract(file_name:str):
    df = pd.read_csv(f"{file_name}")
    print("Extract Sucessfully!!!")
    return df

# Convert broker number that encode to Broker name (Fake data)
def broker_map(df):
    broker_unique = df['brokered_by'].unique()
    broker_mapping = {broker_id: f"Broker {chr(65 + i % 26)}-{i // 26 + 1}" for i, broker_id in enumerate(broker_unique)}
    df['brokered_by'] = df['brokered_by'].map(broker_mapping)
    return df 

def Transform(raw_data):
    raw_data.drop(columns=['prev_sold_date'],axis=1)
    raw_data.fillna( 
       {
           "brokered_by":'Unknown',
           "price":raw_data["price"].mean(),
           'bed':raw_data['bed'].median(),
           'bath':raw_data['bath'].median(),
           'acre_lot':raw_data['acre_lot'].mean(),
           'street': raw_data['street'].mode()[0],
           'city': "Unknown",
           'state':"Unknown",
           # change City and state to lowercase 
           "city": raw_data['city'].str.lower().str.strip(),
           "state": raw_data['state'].str.lower().str.strip(),
           'zip_code':"Unknow",
           'house_size': raw_data['house_size'].mean(),
          
       },inplace = True  
    )
    
    raw_data['price_per_sqft'] = raw_data['price'] / raw_data.house_size
    raw_data['lot_size_in_sqft'] = raw_data['acre_lot'] * 43560
    print("Transform Sucessfully!!!")
    return raw_data

'''''def load_data_to_csv(full_data,full_path):
    # Load TO CSV
    full_data.to_csv(full_path,index = False)
    print("Export Successfully!!!")'''''

def load_data_to_cloud(df, destination_bucket, destination_file):
    """Loads the data to GCP Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(destination_bucket)
    blob = bucket.blob(destination_file)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    print("Load Successfully!!!")

def main():
    get_data = extract('realtor-data.csv')
    decode_broker = broker_map(get_data)
    clean_data = Transform(decode_broker)
    load_data_to_cloud(clean_data,'YOUR_BUCKET_NAME','YOUR_FILE_NAME.csv')
    #load_data_to_csv(clean_data,'real-estate-data01.csv')

if  __name__ == '__main__':
    main()