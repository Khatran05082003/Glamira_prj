import os
from google.cloud import bigquery, storage
import functions_framework

@functions_framework.cloud_event
def auto_load_gcs_to_bigquery(cloud_event):
    """Tự động tạo bảng trong BigQuery với tên từ source_blob_name và tải dữ liệu vào bảng đó."""
    
    # Lấy thông tin từ sự kiện Cloud Storage
    data = cloud_event.data
    bucket_name = data['bucket']
    source_blob_name = data['name']  # Đây là tên file trong bucket GCS
    dataset_id = 'glamira_dataset'
    table_id = os.path.splitext(os.path.basename(source_blob_name))[0]  # Lấy tên bảng từ tên file (không bao gồm phần mở rộng)

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    
    source_uri = f"gs://{bucket_name}/{source_blob_name}"
    
    # Cấu hình job tải dữ liệu vào BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,  # Phát hiện schema tự động
        ignore_unknown_values=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    # Tạo bảng nếu chưa tồn tại
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        # Tạo bảng với schema tự động phát hiện
        table = bigquery.Table(table_ref)
        table = bigquery_client.create_table(table, exists_ok=True)
        print(f"Table {table.table_id} created in dataset {dataset_id}.")
    except Exception as e:
        print(f"An error occurred while creating the table: {e}")
    
    # Tải dữ liệu vào bảng
    try:
        load_job = bigquery_client.load_table_from_uri(
            source_uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config,
        )
        
        print(f"Loading data from {source_uri} into {dataset_id}.{table_id}")
        
        load_job.result()  # Chờ cho job hoàn tất
        
        destination_table = bigquery_client.get_table(f"{dataset_id}.{table_id}")
        print(f"Data loaded into {dataset_id}.{table_id}")
        print(f"Table schema: {[field.name + ':' + field.field_type for field in destination_table.schema]}")
    
    except Exception as e:
        print(f"An error occurred while loading data: {e}")