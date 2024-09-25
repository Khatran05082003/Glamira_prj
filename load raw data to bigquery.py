from google.cloud import bigquery

def load_ndjson_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id):
    # Khởi tạo client BigQuery
    bigquery_client = bigquery.Client()

    # Tạo URI của tệp NDJSON trên GCS
    uri = f'gs://{bucket_name}/{source_blob_name}'

    # Cập nhật cấu hình schema
    job_config = bigquery.LoadJobConfig(
        schema=[
            # Các trường
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
            bigquery.SchemaField('recommendation', 'STRING'),  # Trường này được giữ lại một lần duy nhất
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

            # Cấu trúc RECORD lồng nhau
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
            ]),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,  # Bỏ qua tự động phát hiện schema
        ignore_unknown_values=True,  # Bỏ qua các trường không xác định
        max_bad_records=1000,  # Tăng số lượng bản ghi lỗi tối đa cho phép
    )

    # Tải dữ liệu vào BigQuery
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)

    # Đợi cho đến khi job hoàn thành
    load_job.result()

    # In ra kết quả
    print(f"Hoàn tất tải dữ liệu vào bảng {table_id} trong dataset {dataset_id}. Số lượng dòng đã tải: {load_job.output_rows}")

    # Xem thông tin lỗi nếu có
    errors = load_job.errors
    if errors:
        print("Có lỗi trong quá trình tải dữ liệu:")
        for error in errors:
            print(f"Lỗi: {error['message']}")

# Thay đổi các tham số dưới đây với thông tin của bạn
bucket_name = 'glamira_prj_bucket'
source_blob_name = 'summary.ndjson'
dataset_id = 'glamira_dataset'
table_id = 'glamira_raw'

load_ndjson_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id)
