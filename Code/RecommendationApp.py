import os
import pickle
import streamlit as st
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import pandas as pd
from io import BytesIO
from PIL import Image
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler
from streamlit_searchbox import st_searchbox

# ----- Config -----
SERVICE_ACCOUNT_PATH = r"C:\Users\ACER\Downloads\ServiceAccount.json"
BUCKET_NAME = "glamira-project-storage"
FILE_NAME_HYBRID = "models/recommendation_objects_Hybrid.pkl"
FILE_NAME_ITEM2ITEM = "models/recommendation_objects_Item2Item.pkl"
BANNER_IMAGE_URL = "https://cdn-media.glamira.com/media/bannerslider16/30_april_banner_25_vn.jpg"
LOGO_URL = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSFI95Hs4ynpqN0qZFuO6O58JYWMlDs2kRZTg&s"

# ----- Global Variables -----
user_profiles = None
product_df = None
weighted_tfidf_matrix_products = None
product_indices = None
initial_df = None
svd_model = None
trainset = None
cosine_sim_item_item = None

# ----- GCS & BigQuery Clients -----
@st.cache_resource
def init_gcs_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    return storage.Client()

@st.cache_resource
def init_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

# ----- Load models -----
@st.cache_data(ttl=3600)
def load_from_gcs_hybrid():
    client = init_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME_HYBRID)
    with BytesIO() as f:
        blob.download_to_file(f)
        f.seek(0)
        return pickle.load(f)

@st.cache_data(ttl=3600)
def load_from_gcs_item2item():
    client = init_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME_ITEM2ITEM)
    with BytesIO() as f:
        blob.download_to_file(f)
        f.seek(0)
        return pickle.load(f)

@st.cache_data(ttl=3600)
def load_product_names_from_bigquery():
    client = init_bigquery_client()
    query = """
        SELECT DISTINCT product_name 
        FROM `project2-423018.glamira_dataset_models.Hybrid`
        WHERE product_name IS NOT NULL
    """
    return [row.product_name for row in client.query(query).result()]

# ----- Image & Display -----
def display_product_image(product_id):
    client = init_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    for ext in ['jpg', 'jpeg', 'png']:
        path = f"images/{product_id}.{ext}"
        blob = bucket.blob(path)
        if blob.exists():
            with BytesIO() as f:
                blob.download_to_file(f)
                f.seek(0)
                image = Image.open(f)
                st.image(image, width=200)
                return
    st.image("https://via.placeholder.com/200x200?text=No+Image", width=200)

def product_image_exists(product_id):
    client = init_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    return any(bucket.blob(f"images/{product_id}.{ext}").exists() for ext in ['jpg', 'jpeg', 'png'])

def display_product_card(product, col):
    with col:
        container = st.container(border=True)
        with container:
            display_product_image(product['final_product_id'])
            st.markdown(f"**{product['product_name']}**")
            st.markdown(f"💰 **${product['final_price']:,.2f}**")

# ----- Recommenders -----
def generate_cb_recommendations(user_id, top_n):
    if user_id not in user_profiles:
        return pd.DataFrame()

    profile = user_profiles[user_id].reshape(1, -1)
    scores = cosine_similarity(profile, weighted_tfidf_matrix_products)[0]
    scores = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

    interacted = set(initial_df[(initial_df['email_address'] == user_id) &
                                (initial_df['interaction_type'] == 'checkout_success')]['final_product_id'].astype(str))
    recs = []
    for i, score in scores:
        pid = str(product_df.iloc[i]['final_product_id'])
        if pid not in interacted:
            recs.append({
                'final_product_id': pid,
                'product_name': product_df.iloc[i]['product_name'],
                'final_price': product_df.iloc[i]['final_price'],
                'similarity_score': score
            })
        if len(recs) == top_n * 2:  # Get more items to ensure we have enough with images
            break
    return pd.DataFrame(recs)

def generate_cf_recommendations(user_id, top_n):
    try:
        uid = trainset.to_inner_uid(user_id)
    except ValueError:
        return pd.DataFrame()
    unseen = set(trainset.all_items()) - {j for (j, _) in trainset.ur[uid]}
    preds = [svd_model.predict(user_id, trainset.to_raw_iid(i)) for i in unseen]
    preds = sorted(preds, key=lambda x: x.est, reverse=True)[:top_n * 2]  # Get more items
    df = pd.DataFrame([{'final_product_id': p.iid, 'estimated_rating': p.est} for p in preds])
    return df.merge(product_df[['final_product_id', 'product_name', 'final_price']], on='final_product_id')

def generate_hybrid_recommendations(user_id, top_n):
    alpha = 0.7  # Fixed alpha value
    cb = generate_cb_recommendations(user_id, top_n)
    cf = generate_cf_recommendations(user_id, top_n)
    if cb.empty and cf.empty:
        return pd.DataFrame()

    scaler = MinMaxScaler()
    if not cb.empty:
        cb['cb_score'] = scaler.fit_transform(cb[['similarity_score']])
    if not cf.empty:
        cf['cf_score'] = scaler.fit_transform(cf[['estimated_rating']])
    
    cb = cb[['final_product_id', 'cb_score']]
    cf = cf[['final_product_id', 'cf_score']]
    df = pd.merge(cb, cf, on='final_product_id', how='outer').fillna(0)
    df['final_score'] = alpha * df['cb_score'] + (1 - alpha) * df['cf_score']
    return df.merge(product_df, on='final_product_id').sort_values(by='final_score', ascending=False).head(top_n * 2)  # Get more items

# ----- Item-to-Item Similarity -----
def get_similar_items(product_id_input, top_n=10):
    pid = str(product_id_input)
    if pid not in product_indices:
        return pd.DataFrame()
    idx = product_indices[pid]
    sim_scores = list(enumerate(cosine_sim_item_item[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)[1:]
    recs = []
    for i, score in sim_scores:
        row = product_df.iloc[i]
        recs.append({
            "final_product_id": row["final_product_id"],
            "product_name": row["product_name"],
            "final_price": row["final_price"],
            "similarity_score": score
        })
        if len(recs) == top_n * 2:  # Get more items to ensure we have enough with images
            break
    return pd.DataFrame(recs)

# ----- Streamlit UI -----
def main():
    global user_profiles, product_df, weighted_tfidf_matrix_products, product_indices, initial_df, svd_model, trainset, cosine_sim_item_item

    # Custom CSS for styling
    st.markdown("""
<style>
    /* Toàn bộ nền web pastel */
    .stApp {
        background-color: #fef4f4;
        font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', sans-serif;
    }

    /* Header chính 💎 */
    .header {
        color: #4a4a4a;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #f6d365 0%, #fda085 100%);
        border-radius: 15px;
        margin-bottom: 2rem;
        font-size: 26px;
        font-weight: bold;
        letter-spacing: 1px;
    }

    /* Thẻ sản phẩm */
    .product-card {
        transition: transform .2s;
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 20px;
        box-shadow: 0 6px 12px rgba(0,0,0,0.08);
        background-color: #ffffff;
    }
    .product-card:hover {
        transform: scale(1.03);
        box-shadow: 0 12px 24px rgba(0,0,0,0.15);
    }

    /* Khung tìm kiếm & kết quả */
    .search-box, .recommendation-section {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.05);
        margin-bottom: 2rem;
    }

    /* Fix cho searchbox */
    .stSearchbox > label {
        font-size: 16px !important;
        font-weight: 600 !important;
        color: #555 !important;
        margin-bottom: 8px;
    }
    .stSearchbox input {
        font-size: 15px !important;
        padding: 10px 14px !important;
        border-radius: 8px !important;
        border: 1px solid #ccc !important;
    }
    .stSearchbox input::placeholder {
        font-style: italic;
        color: #aaa;
    }

    /* Sidebar */
    .st-emotion-cache-6qob1r {
        background-color: #fff9f9 !important;
    }

    .sidebar-logo {
        width: 180px !important;
        display: block;
        margin: 0 auto 20px auto;
        border-radius: 8px;
    }

    /* Banner */
    .banner-container img {
        border-radius: 15px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }

    /* Button primary */
    .stButton > button {
        background-color: #fda085 !important;
        color: white !important;
        font-weight: bold;
        border-radius: 8px;
        padding: 10px 16px;
    }

    .stButton > button:hover {
        background-color: #f6a278 !important;
    }

</style>
""", unsafe_allow_html=True)


    # Set page config
    st.set_page_config(
        page_title="Glamira Recommender",
        page_icon="💎",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Banner Image
    st.image(BANNER_IMAGE_URL, use_container_width=True)

    # Sidebar with logo
    with st.sidebar:
        st.image(LOGO_URL, width=180)  # Adjusted logo size
        
        st.markdown("## 💎 Glamira Recommender")
        st.markdown("Hệ thống đề xuất sản phẩm trang sức cá nhân hóa")
        st.markdown("---")
        st.markdown("### 🔧 Tùy chọn")
        top_n = st.slider("Số lượng sản phẩm đề xuất", 1, 10, 5)
        st.markdown("---")
        st.markdown("### ℹ️ Thông tin")
        st.markdown("Phiên bản 2.0")
        st.markdown("Dữ liệu cập nhật hàng ngày")

    # Main content
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("<div class='header'><h1>💎 Hệ Thống Đề Xuất Sản Phẩm Glamira</h1></div>", unsafe_allow_html=True)
    with col2:
        st.markdown("")
        st.markdown("")

    # Loading data
    with st.spinner("🔄 Đang tải dữ liệu từ hệ thống..."):
        hybrid = load_from_gcs_hybrid()
        item2item = load_from_gcs_item2item()
        product_names = load_product_names_from_bigquery()

    # Assign variables
    user_profiles = hybrid['user_profiles']
    product_df = hybrid['product_df']
    weighted_tfidf_matrix_products = hybrid['weighted_tfidf_matrix_products']
    product_indices = hybrid['product_indices']
    initial_df = hybrid['initial_df']
    svd_model = hybrid['svd_model']
    trainset = hybrid['data']
    cosine_sim_item_item = item2item['cosine_sim_item_item']

    # Tab layout
    tab1, tab2 = st.tabs(["🎯 Đề xuất theo khách hàng", "🔍 Tìm sản phẩm tương tự"])

    with tab1:
        st.markdown("<div class='search-box'>", unsafe_allow_html=True)
        st.markdown("### 📧 Tìm kiếm khách hàng")
        email = st.text_input("Nhập email khách hàng", placeholder="customer@example.com")
        
        if st.button("🔍 Tìm kiếm", type="primary", use_container_width=True):
            st.markdown("</div>", unsafe_allow_html=True)
            
            if email:
                with st.spinner("🔄 Đang phân tích và đề xuất sản phẩm..."):
                    recs = generate_hybrid_recommendations(email, top_n=top_n * 2)
                    products_with_images = [
                        row for _, row in recs.iterrows() if product_image_exists(row['final_product_id'])
                    ][:top_n]

                if products_with_images:
                    st.markdown("<div class='recommendation-section'>", unsafe_allow_html=True)
                    st.markdown(f"### 🎁 Sản phẩm đề xuất cho khách hàng")
                    st.markdown(f"*Email: {email}*")
                    
                    num_columns = 5
                    for i in range(0, len(products_with_images), num_columns):
                        cols = st.columns(num_columns)
                        for col, item in zip(cols, products_with_images[i:i + num_columns]):
                            display_product_card(item, col)
                    st.markdown("</div>", unsafe_allow_html=True)
                else:
                    st.warning("Không tìm thấy sản phẩm nào có ảnh để hiển thị")
            else:
                st.warning("Vui lòng nhập email khách hàng")
        else:
            st.markdown("</div>", unsafe_allow_html=True)

    with tab2:
        st.markdown("<div class='search-box'>", unsafe_allow_html=True)
        st.markdown("### 🔍 Tìm kiếm sản phẩm")  # Tiêu đề đẹp hơn

        selected_product = st_searchbox(
            lambda q: [p for p in product_names if q.lower() in p.lower()],
            key="product_searchbox",
            placeholder="Nhập tên sản phẩm...",
        )
        st.markdown("</div>", unsafe_allow_html=True)


        if selected_product:
            st.markdown("<div class='recommendation-section'>", unsafe_allow_html=True)
            st.markdown(f"### 📦 Sản phẩm được chọn")
            
            selected_row = product_df[product_df['product_name'] == selected_product].iloc[0]
            col1, col2 = st.columns([1, 3])
            with col1:
                display_product_image(selected_row['final_product_id'])
            with col2:
                st.markdown(f"**{selected_row['product_name']}**")
                st.markdown(f"💰 **Giá:** ${selected_row['final_price']:,.2f}")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div class='recommendation-section'>", unsafe_allow_html=True)
            st.markdown(f"### 🧲 Sản phẩm tương tự")
            
            similar_df = get_similar_items(selected_row['final_product_id'], top_n=top_n * 2)
            products_with_images = [
                row for _, row in similar_df.iterrows() if product_image_exists(row['final_product_id'])
            ][:top_n]

            if products_with_images:
                num_columns = 5
                for i in range(0, len(products_with_images), num_columns):
                    cols = st.columns(num_columns)
                    for col, item in zip(cols, products_with_images[i:i + num_columns]):
                        display_product_card(item, col)
            else:
                st.info("⚠️ Không tìm thấy sản phẩm tương tự nào có ảnh")
            st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()