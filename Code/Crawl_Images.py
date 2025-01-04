import os
import requests
from bs4 import BeautifulSoup
from google.cloud import storage
import xml.etree.ElementTree as ET

# Hàm tải và phân tích sitemap XML với namespace
def get_sitemap_urls(sitemap_url):
    response = requests.get(sitemap_url)
    if response.status_code == 200:
        try:
            tree = ET.ElementTree(ET.fromstring(response.content))
            root = tree.getroot()
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            urls = [url.text for url in root.findall(".//ns:loc", namespace)]
            return urls
        except ET.ParseError:
            return []
    else:
        return []

def crawl_images(url):
    # Gửi yêu cầu GET mà không có headers
    response = requests.get(url)

    # Kiểm tra trạng thái phản hồi
    if response.status_code == 200:
        # Phân tích HTML
        soup = BeautifulSoup(response.content, "html.parser")

        # Tìm tất cả các thẻ <img>
        images = soup.find_all("img")

        # Lọc các ảnh có đuôi .png, .jpeg, .jpg và có từ khóa 'width' hoặc 'height' trong URL
        valid_image_links = set(  # Sử dụng set để đảm bảo các liên kết là duy nhất
            img["src"] for img in images
            if "src" in img.attrs and
               any(ext in img["src"].lower() for ext in [".png", ".jpeg", ".jpg"]) and
               ("https://cdn-media.glamira.com/media/product/" in img["src"])
        )

        # In ra danh sách link ảnh hợp lệ
        print("Link ảnh hợp lệ tìm thấy:")
        for link in valid_image_links:
            print(link)
        return list(valid_image_links)
    else:
        print(f"Lỗi khi truy cập trang web: {response.status_code}")

# Hàm kiểm tra đuôi ảnh hợp lệ và có từ khóa 'width' hoặc 'height' trong URL
def is_valid_image_url(img_url):
    valid_extensions = ['.jpg', '.jpeg', '.png']
    return any(img_url.lower().endswith(ext) for ext in valid_extensions) and ('https://cdn-media.glamira.com/media/product/' in img_url)

# Hàm crawl các ảnh trong trang web và upload lên Google Cloud Storage
def crawl_and_upload_images(page_url, storage_client, bucket_name):
    response = requests.get(page_url)
    if response.status_code == 200:
        img_urls = crawl_images(page_url)

        # In ra số lượng ảnh tìm được và danh sách link ảnh
        print(f"Tìm thấy {len(img_urls)} ảnh hợp lệ trong trang: {page_url}")
        print(f"Danh sách các link ảnh: {img_urls}")

        # Tạo thư mục chính trong Cloud Storage
        folder_path = 'images'  # Dùng thư mục 'images' chung cho tất cả ảnh
        bucket = storage_client.bucket(bucket_name)

        # Lấy tên sản phẩm từ URL (sau "https://www.glamira.com/" và trước ".html")
        product_name = page_url.split('https://www.glamira.com/')[1].split('.html')[0]

        # Duyệt qua từng ảnh và upload lên Cloud Storage
        for img_url in set(img_urls):  # Dùng set để loại bỏ các link trùng lặp
            try:
                # Kiểm tra nếu ảnh có URL đầy đủ
                if not img_url.startswith('http'):
                    img_url = 'https:' + img_url

                # Tải ảnh về
                img_data = requests.get(img_url).content
                img_name = os.path.basename(img_url)

                # Đổi tên file ảnh theo tên sản phẩm + tên ảnh gốc
                new_img_name = f"{product_name}"

                # Đẩy ảnh lên Cloud Storage vào thư mục 'images'
                blob = bucket.blob(f"{folder_path}/{new_img_name}")
                blob.upload_from_string(img_data)

                # In ra đường link của ảnh đã upload
                image_link = f"gs://{bucket_name}/{folder_path}/{new_img_name}"
                print(f"Đã upload ảnh: {new_img_name} vào {image_link}")
            except Exception as e:
                print(f"Không thể tải hoặc upload ảnh {img_url}: {e}")
    else:
        print(f"Không thể tải trang web: {page_url}")

# Hàm chính để thực hiện các bước trên
def main():
    # URL của các sitemap cần crawl (product_provider-41-1 đến product_provider-41-9)
    base_url = 'https://www.glamira.com/media/sitemap/glus/product_provider-41-'
    sitemap_urls = [f"{base_url}{i}.xml" for i in range(1, 10)]

    # Khởi tạo client Google Cloud Storage
    storage_client = storage.Client()
    bucket_name = 'glamira-prj1'  # Thay bằng tên bucket của bạn

    # Duyệt qua từng sitemap
    for sitemap_url in sitemap_urls:
        # Lấy danh sách các URL sản phẩm từ sitemap
        product_urls = get_sitemap_urls(sitemap_url)

        # Kiểm tra nếu không có URL sản phẩm nào
        if not product_urls:
            continue

        # Duyệt qua từng URL sản phẩm và crawl ảnh
        for product_url in product_urls:
            crawl_and_upload_images(product_url, storage_client, bucket_name)

if __name__ == "__main__":
    main()
