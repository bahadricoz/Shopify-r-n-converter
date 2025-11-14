import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from converter import shopify_to_ikas_converter

st.set_page_config(page_title="Shopify → ikas Dönüştürücü", page_icon="🛒")

st.title("Shopify → ikas Ürün Dönüşüm Aracı")
st.write(
    """
    Shopify ürün dışa aktarma dosyanızı (CSV veya XLSX) yükleyin. Uygulama, dosyayı
    ikas ürün içe aktarma şemasına dönüştürür ve çıktı dosyasını indirmenizi sağlar.
    
    **Özellikler:**
    - Varyantlı ürünler için otomatik Grup ID oluşturma
    - Görsel URL'lerini otomatik birleştirme (Image Src + Variant Image)
    - Varyantlı ürünlerde ortak bilgileri tüm satırlara uygulama
    - CSV ve Excel formatında indirme desteği
    """
)

uploaded_file = st.file_uploader(
    "Shopify ürün dosyasını yükleyin", type=["csv", "xlsx", "xls"], accept_multiple_files=False
)

if uploaded_file is not None:
    file_suffix = Path(uploaded_file.name).suffix.lower()

    with tempfile.NamedTemporaryFile(delete=False, suffix=file_suffix) as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        tmp_path = tmp_file.name

    try:
        converted_df = shopify_to_ikas_converter(tmp_path)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Dönüşüm sırasında hata oluştu: {exc}")
    else:
        st.success("Dönüşüm başarılı! Aşağıdaki tabloyu kontrol edin ve indir butonlarını kullanın.")
        st.dataframe(converted_df, use_container_width=True)

        csv_bytes = converted_df.to_csv(index=False).encode("utf-8-sig")

        csv_file_name = f"ikas_donusum_{Path(uploaded_file.name).stem}.csv"
        st.download_button(
            label="CSV olarak indir",
            data=csv_bytes,
            file_name=csv_file_name,
            mime="text/csv",
        )

        # Excel indirme seçeneği
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            converted_df.to_excel(writer, index=False, sheet_name="ikas_products")
        excel_buffer.seek(0)

        excel_file_name = f"ikas_donusum_{Path(uploaded_file.name).stem}.xlsx"
        st.download_button(
            label="Excel olarak indir",
            data=excel_buffer,
            file_name=excel_file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    finally:
        Path(tmp_path).unlink(missing_ok=True)
else:
    st.info("Başlamak için Shopify ürün dosyanızı yükleyin.")
