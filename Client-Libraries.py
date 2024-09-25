from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1

# Function to query data from BigQuery
def query_bigquery():
    client = bigquery.Client()

    query = """
    SELECT name, SUM(number) as total
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE state = 'TX'
    GROUP BY name
    ORDER BY total DESC
    LIMIT 10
    """
    
    query_job = client.query(query)
    
    print("Top 10 baby names in Texas:")
    for row in query_job:
        print(f"{row.name}: {row.total}")

# Function to upload a file to Google Cloud Storage
def upload_to_cloud_storage(bucket_name, file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(file_path)

    print(f"File {file_path} uploaded to {destination_blob_name}.")

# Function to publish a message to a Pub/Sub topic
def publish_message(topic_name, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('your-project-id', topic_name)

    future = publisher.publish(topic_path, message.encode("utf-8"))
    print(f"Published message to {topic_name}: {future.result()}")

if __name__ == "__main__":
    # 1. Query data from BigQuery
    query_bigquery()

    # 2. Upload a file to Google Cloud Storage
    bucket_name = "your-bucket-name"
    file_path = "path/to/your/file.txt"
    destination_blob_name = "uploaded-file.txt"
    upload_to_cloud_storage(bucket_name, file_path, destination_blob_name)

    # 3. Publish a message to Pub/Sub
    topic_name = "your-topic-name"
    message = "Hello, Google Cloud Pub/Sub!"
    publish_message(topic_name, message)