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
    if source_blob_name == 'summary.ndjson':
        schema = [
            bigquery.SchemaField('_id', 'JSON'),
            bigquery.SchemaField('time_stamp', 'JSON'),
            bigquery.SchemaField('recommendation_product_id', 'STRING'),
            bigquery.SchemaField('viewing_product_id', 'STRING'),
            bigquery.SchemaField('user_agent', 'STRING'),
            bigquery.SchemaField('collect_id', 'STRING'),
            bigquery.SchemaField('product_id', 'INTEGER'),
            bigquery.SchemaField('recommendation_clicked_position', 'JSON'),
            bigquery.SchemaField('collection', 'STRING'),
            bigquery.SchemaField('utm_source', 'STRING'),
            bigquery.SchemaField('recommendation', 'STRING'),
            bigquery.SchemaField('utm_medium', 'STRING'),
            bigquery.SchemaField('show_recommendation', 'STRING'),
            bigquery.SchemaField('user_id_db', 'STRING'),
            bigquery.SchemaField('local_time', 'STRING'),
            bigquery.SchemaField('email_address', 'STRING'),
            bigquery.SchemaField('referrer_url', 'STRING'),
            bigquery.SchemaField('cat_id', 'STRING'),
            bigquery.SchemaField('is_paypal', 'BOOLEAN'),
            bigquery.SchemaField('device_id', 'STRING'),
            bigquery.SchemaField('key_search', 'STRING'),
            bigquery.SchemaField('current_url', 'STRING'),
            bigquery.SchemaField('api_version', 'STRING'),
            bigquery.SchemaField('resolution', 'STRING'),
            bigquery.SchemaField('order_id', 'JSON'),
            bigquery.SchemaField('ip', 'STRING'),
            bigquery.SchemaField('store_id', 'STRING'),
            bigquery.SchemaField('recommendation_product_position', 'JSON'),
            bigquery.SchemaField('price', 'STRING'),
            bigquery.SchemaField('currency', 'STRING'),
            bigquery.SchemaField('amount', 'INTEGER'),
            bigquery.SchemaField('cart_products', 'RECORD', mode='REPEATED', fields=[
                bigquery.SchemaField('product_id', 'JSON'),
                bigquery.SchemaField('price', 'STRING'),
                bigquery.SchemaField('currency', 'STRING'),
                bigquery.SchemaField('amount', 'JSON'),
                bigquery.SchemaField('option', 'JSON'),
            ]),
            bigquery.SchemaField('option', 'RECORD', mode='REPEATED', fields=[
                bigquery.SchemaField('option_label', 'STRING'),
                bigquery.SchemaField('option_id', 'STRING'),
                bigquery.SchemaField('value_label', 'STRING'),
                bigquery.SchemaField('value_id', 'STRING'),
                bigquery.SchemaField('quality', 'STRING'),
                bigquery.SchemaField('quality_label', 'STRING'),
                bigquery.SchemaField('alloy', 'STRING'),
                bigquery.SchemaField('diamond', 'STRING'),
                bigquery.SchemaField('shapediamond', 'STRING'),
                bigquery.SchemaField('stone', 'STRING'),
                bigquery.SchemaField('pearlcolor', 'STRING'),
                bigquery.SchemaField('finish', 'STRING'),
                bigquery.SchemaField('price', 'STRING'),
                bigquery.SchemaField('currency', 'STRING')
            ])
        ]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            ignore_unknown_values=True,
            max_bad_records=1000,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            ignore_unknown_values=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    # Tạo bảng nếu chưa tồn tại
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
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
