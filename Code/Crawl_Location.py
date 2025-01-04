import os
import IP2Location
import json
from google.cloud import storage
import collection.connect_mongodb as connect_mongodb
import time
from pymongo import errors
from IPython.display import clear_output

# Khởi tạo database IP2Location
database_v4 = IP2Location.IP2Location(r"D:\project_glamira\IP2Location-Python-master\data\need\IP2LOCATION-LITE-DB11.BIN")
database_v6 = IP2Location.IP2Location(r"D:\project_glamira\IP2Location-Python-master\data\need\IP2LOCATION-LITE-DB11.IPV6.BIN")

# Đường dẫn file NDJSON
ndjson_file_path = "location.ndjson"

# Tạo hoặc cập nhật file NDJSON
def append_to_ndjson(file_path, data):
    with open(file_path, 'a', encoding='utf-8') as file:
        file.write(json.dumps(data, ensure_ascii=False) + '\n')

# Khởi tạo từ điển lưu IP
ip_list = {}

# Đọc dữ liệu từ file NDJSON nếu đã tồn tại
if os.path.exists(ndjson_file_path):
    with open(ndjson_file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                record = json.loads(line)
                ip_list[record['ip']] = record
            except json.JSONDecodeError:
                continue
print(f"Total IPs loaded from NDJSON: {len(ip_list)}")

# Hàm xử lý thông tin IPv6
def take_info_ipv6(initial_doc):
    global ip_list
    ip = initial_doc.get("ip")
    if ip in ip_list:
        return
    rec = database_v6.get_all(ip)
    document = {
        'ip': ip,
        'country_code': rec.country_short,
        'country_name': rec.country_long,
        'region_name': rec.region,
        'city_name': rec.city,
        'latitude': rec.latitude,
        'longtitude': rec.longitude,
        'ZIP_code': rec.zipcode,
        'time_zone': rec.timezone,
    }
    ip_list[ip] = document
    append_to_ndjson(ndjson_file_path, document)

# Hàm xử lý thông tin IPv4
def take_info_ipv4(initial_doc):
    global ip_list
    ip = initial_doc.get("ip")
    if ip in ip_list:
        return
    rec = database_v4.get_all(ip)
    document = {
        'ip': ip,
        'country_code': rec.country_short,
        'country_name': rec.country_long,
        'region_name': rec.region,
        'city_name': rec.city,
        'latitude': rec.latitude,
        'longtitude': rec.longitude,
        'ZIP_code': rec.zipcode,
        'time_zone': rec.timezone,
    }
    if document.get('country_code') == "IPV6 ADDRESS MISSING IN IPV4 BIN":
        take_info_ipv6(initial_doc)
    else:
        ip_list[ip] = document
        append_to_ndjson(ndjson_file_path, document)

# Hàm chính
def main_code():
    collection = connect_mongodb.connect_db()
    documents = collection.find({}, {"ip": 1, "_id": 0})
    doc = 0
    for document in documents:
        doc += 1
        take_info_ipv4(document)
        print(f'Document processed: {doc}')
        if doc % 2000 == 0:
            print(f"Total IPs processed: {len(ip_list)}")
            print("Sleeping for 10 seconds...")
            time.sleep(10)
            clear_output()
    print(f"Total documents processed: {doc}")

# Tải file lên Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to GCS as {destination_blob_name}.")

# Vòng lặp chính
while True:
    try:
        main_code()
    except errors.CursorNotFound as e:
        print(f"Cursor not found: {e}")
        time.sleep(10)
        continue
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        time.sleep(5)
        continue
    break

# Tải tệp lên GCS sau khi xử lý xong
bucket_name = "glamira-prj1"  # Tên bucket GCS
upload_to_gcs(bucket_name, ndjson_file_path, "location.ndjson")
