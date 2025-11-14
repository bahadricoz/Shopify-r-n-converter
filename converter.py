# Standard library imports
import pathlib
from typing import List, Set

import pandas as pd


# Shopify sütun isimleri (kullanıcının belirttiği sütunlar)
SHOPIFY_COLUMNS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Type",
    "Product Category",
    "Tags",
    "Published",
    "Status",  # Yeni: Satış Kanalı için
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Barcode",
    "Barcode",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Price",
    "Compare At Price",
    "Variant Compare At Price",
    "Image Src",
    "Image Position",
    "Image Alt Text",
    "Variant Image",
    "SEO Title",
    "SEO Description",
    "Created At",
    "Google Shopping / Google Product Category",
]

# ikas sütun yapısı (kullanıcının belirttiği TAM VE KESİN liste - 37 sütun)
IKAS_COLUMNS = [
    "Ürün Grup ID",
    "Varyant ID",
    "İsim",
    "Açıklama",
    "Satış Fiyatı",
    "İndirimli Fiyatı",
    "Alış Fiyatı",
    "Barkod Listesi",
    "SKU",
    "Silindi mi?",
    "Marka",
    "Kategoriler",
    "Etiketler",
    "Resim URL",
    "Metadata Başlık",
    "Metadata Açıklama",
    "Slug",
    "Stok:Ana Depo",
    "Tip",
    "Varyant Tip 1",
    "Varyant Değer 1",
    "Varyant Tip 2",
    "Varyant Değer 2",
    "Desi",
    "HS Kod",
    "Birim Ürün Miktarı",
    "Ürün Birimi",
    "Satılan Ürün Miktarı",
    "Satılan Ürün Birimi",
    "Google Ürün Kategorisi",
    "Tedarikçi",
    "Stoğu Tükenince Satmaya Devam Et",
    "Satış Kanalı:belix",
    "Sepet Başına Minimum Alma Adeti:belix",
    "Sepet Başına Maksimum Alma Adeti:belix",
    "Varyant Aktiflik",
    "Oluşturulma Tarihi",
]


def shopify_to_ikas_converter(file_path: str) -> pd.DataFrame:
    """Read a Shopify export file and convert it into the ikas schema.

    Bu fonksiyon aşağıdaki özel kurallara uyar:
    1. Basit ürün kontrolü ve birleştirme: Option Value "Default Title" ise basit üründür - Shopify'daki tüm satırlar TEK SATIRDA birleştirilir
    2. Basit ürünler için: Varyant Tip/Değer sütunları boş, Ürün Grup ID boş
    3. Varyantlı ürünler için: Aynı Handle'a sahip ürünler aynı Ürün Grup ID'ye sahip olur
    4. Varyant Satır Sayısı Garanti: Varyantlı ürünler için ikas dosyasındaki satır sayısı Shopify'daki fiili varyant kombinasyonlarına eşittir (her varyant = 1 satır, fazladan satır eklenmez)
    4a. Varyant Birleştirme: Aynı varyant kombinasyonuna (Option1 Value + Option2 Value) sahip satırlar tek satırda birleştirilir (görseller için tekrarlanan satırlar birleştirilir)
    5. Varyant Tip tekrarı: Varyant Tip 1/2 sütunları, o varyantın değerinin bulunduğu HER satırda tekrarlanarak dolu gelir
    6. Varyant Tip/Değer eşleştirmesi: Option1 Name/Value → Varyant Tip 1/Değer 1, Option2 Name/Value → Varyant Tip 2/Değer 2
    7. Slug sütunu Handle'dan otomatik oluşturulur
    8. Kategoriler bilgisi Product Category'den alınır (yoksa Type kullanılır)
    9. Fiyat eşleştirmesi: Variant Price → Satış Fiyatı, Compare At Price → İndirimli Fiyatı
    10. Varyantlı ürünlerde ortak bilgiler (İsim, Açıklama, Kategoriler, Etiketler, Marka) tüm satırlarda aynıdır
    11. Resim URL (Image Src + Variant Image) toplanıp noktalı virgülle birleştirilir ve tüm satırlarda (basit+varyantlı) tekrarlanır
    12. Metadata: SEO Title → Metadata Başlık, SEO Description → Metadata Açıklama
    13. Barkod Listesi: Variant Barcode veya Barcode sütunundan alınır
    14. Satış Kanalı:belix: Status/Published "Active" ise TÜM satırlara (basit+varyantlı) "VISIBLE" yazılır
    15. Varyant Aktiflik: Boş bırakılır

    Parameters
    ----------
    file_path : str
        Absolute or relative path to a Shopify export file in CSV or XLSX format.

    Returns
    -------
    pd.DataFrame
        DataFrame whose columns follow the ikas import schema.
    """

    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    # Dosyayı oku
    if path.suffix.lower() == ".csv":
        source_df = pd.read_csv(path)
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        source_df = pd.read_excel(path)
    else:
        raise ValueError("Unsupported file extension. Please provide CSV or XLSX.")

    # Handle sütunu zorunlu
    if "Handle" not in source_df.columns:
        raise ValueError("Handle sütunu bulunamadı. Lütfen geçerli bir Shopify export dosyası yükleyin.")

    # Boş Handle değerlerini doldur (önceden gelen değeri kullan)
    source_df["Handle"] = source_df["Handle"].ffill()

    # Handle'a göre grupla ve ortak bilgileri topla
    # Önce her Handle için ortak bilgileri belirle
    handle_groups = source_df.groupby("Handle", sort=False)

    # Ortak bilgileri topla (her Handle için ilk boş olmayan değeri al)
    common_info = {}
    image_urls = {}
    handle_status = {}  # Her Handle için Status bilgisini sakla

    for handle, group_df in handle_groups:
        # Ortak bilgiler (ilk boş olmayan değeri kullan)
        # Kategori için önce Product Category'yi dene, yoksa Type'ı kullan
        category_col = None
        if "Product Category" in group_df.columns and not group_df["Product Category"].dropna().empty:
            category_col = "Product Category"
        elif "Type" in group_df.columns and not group_df["Type"].dropna().empty:
            category_col = "Type"
        
        # Metadata bilgilerini topla
        seo_title = ""
        seo_description = ""
        if "SEO Title" in group_df.columns and not group_df["SEO Title"].dropna().empty:
            seo_title = group_df["SEO Title"].dropna().iloc[0]
        if "SEO Description" in group_df.columns and not group_df["SEO Description"].dropna().empty:
            seo_description = group_df["SEO Description"].dropna().iloc[0]
        
        # Google Ürün Kategorisi
        google_category = ""
        if "Google Shopping / Google Product Category" in group_df.columns and not group_df["Google Shopping / Google Product Category"].dropna().empty:
            google_category = group_df["Google Shopping / Google Product Category"].dropna().iloc[0]
        elif "Google Product Category" in group_df.columns and not group_df["Google Product Category"].dropna().empty:
            google_category = group_df["Google Product Category"].dropna().iloc[0]
        
        # Tip (Type)
        tip_value = ""
        if "Type" in group_df.columns and not group_df["Type"].dropna().empty:
            tip_value = group_df["Type"].dropna().iloc[0]
        
        # Oluşturulma Tarihi
        created_at = ""
        if "Created At" in group_df.columns and not group_df["Created At"].dropna().empty:
            created_at = group_df["Created At"].dropna().iloc[0]
        
        # Status/Published bilgisini topla (Handle seviyesinde)
        status_value = None
        # Önce Status sütununu kontrol et
        if "Status" in group_df.columns and not group_df["Status"].dropna().empty:
            status_value = str(group_df["Status"].dropna().iloc[0]).strip().upper()
        elif "Published" in group_df.columns and not group_df["Published"].dropna().empty:
            published = str(group_df["Published"].dropna().iloc[0]).strip().upper()
            # Published TRUE ise Active olarak kabul et
            if published in ["TRUE", "1", "YES"]:
                status_value = "ACTIVE"
        
        handle_status[handle] = status_value == "ACTIVE" if status_value else False
        
        common_info[handle] = {
            "Title": group_df["Title"].dropna().iloc[0] if "Title" in group_df.columns and not group_df["Title"].dropna().empty else "",
            "Body (HTML)": group_df["Body (HTML)"].dropna().iloc[0] if "Body (HTML)" in group_df.columns and not group_df["Body (HTML)"].dropna().empty else "",
            "Category": group_df[category_col].dropna().iloc[0] if category_col else "",
            "Tags": group_df["Tags"].dropna().iloc[0] if "Tags" in group_df.columns and not group_df["Tags"].dropna().empty else "",
            "Vendor": group_df["Vendor"].dropna().iloc[0] if "Vendor" in group_df.columns and not group_df["Vendor"].dropna().empty else "",
            "SEO Title": seo_title,
            "SEO Description": seo_description,
            "Google Category": google_category,
            "Type": tip_value,
            "Created At": created_at,
        }

        # Görsel URL'lerini topla (Image Src + Variant Image)
        image_urls_set: Set[str] = set()

        if "Image Src" in group_df.columns:
            image_srcs = group_df["Image Src"].dropna().astype(str)
            image_urls_set.update(image_srcs[image_srcs != ""])

        if "Variant Image" in group_df.columns:
            variant_images = group_df["Variant Image"].dropna().astype(str)
            image_urls_set.update(variant_images[variant_images != ""])

        # Noktalı virgülle birleştir (boşluksuz)
        image_urls[handle] = ";".join(sorted(image_urls_set)) if image_urls_set else ""

    # Yeni DataFrame oluştur
    ikas_rows = []

    for handle, group_df in handle_groups:
        # Grup ID (Handle'ı kullan, benzersiz olması için)
        grup_id = handle

        # Ortak bilgiler
        common = common_info[handle]
        images = image_urls[handle]

        # Basit ürün kontrolü: Eğer Option Value "Default Title" ise basit üründür
        is_simple_product = False
        # İlk satırı kontrol et (tüm satırlar aynı Handle'a sahip olduğu için ilk satır yeterli)
        first_row = group_df.iloc[0]
        if "Option1 Value" in first_row and pd.notna(first_row["Option1 Value"]):
            if str(first_row["Option1 Value"]).strip().upper() == "DEFAULT TITLE":
                is_simple_product = True
        
        # Basit ürün ise Grup ID'yi boş bırak
        if is_simple_product:
            grup_id = ""
            
            # BASİT ÜRÜN: Tüm satırları birleştirip TEK SATIR oluştur
            # Tüm satırlardaki bilgileri birleştir
            variant_sku = ""
            barcode = ""
            sale_price = 0.0
            discounted_price = 0.0
            stock_qty = 0
            
            # İlk boş olmayan değerleri al
            for idx, row in group_df.iterrows():
                if not variant_sku and "Variant SKU" in row and pd.notna(row["Variant SKU"]):
                    variant_sku = str(row["Variant SKU"])
                
                if not barcode:
                    if "Variant Barcode" in row and pd.notna(row["Variant Barcode"]):
                        barcode = str(row["Variant Barcode"])
                    elif "Barcode" in row and pd.notna(row["Barcode"]):
                        barcode = str(row["Barcode"])
                
                if sale_price == 0.0:
                    if "Variant Price" in row and pd.notna(row["Variant Price"]):
                        try:
                            sale_price = float(row["Variant Price"])
                        except (ValueError, TypeError):
                            pass
                
                if discounted_price == 0.0:
                    compare_price = row.get("Compare At Price", None)
                    if compare_price is None or pd.isna(compare_price):
                        compare_price = row.get("Variant Compare At Price", None)
                    if compare_price is not None and pd.notna(compare_price):
                        try:
                            discounted_price = float(compare_price)
                        except (ValueError, TypeError):
                            pass
                
                if stock_qty == 0:
                    if "Variant Inventory Qty" in row and pd.notna(row["Variant Inventory Qty"]):
                        try:
                            stock_qty = int(row["Variant Inventory Qty"])
                        except (ValueError, TypeError):
                            pass
            
            # Satış Kanalı: Handle seviyesinde Status kontrolü
            satis_kanali = "VISIBLE" if handle_status[handle] else ""
            
            # Basit ürün için TEK SATIR oluştur
            ikas_row = {
                "Ürün Grup ID": "",  # Basit ürün için boş
                "Varyant ID": "",  # Boş bırak
                "İsim": common["Title"],
                "Açıklama": common["Body (HTML)"],
                "Satış Fiyatı": sale_price,
                "İndirimli Fiyatı": discounted_price,
                "Alış Fiyatı": "",  # Boş
                "Barkod Listesi": barcode,
                "SKU": variant_sku,
                "Silindi mi?": "",  # Boş
                "Marka": common["Vendor"],
                "Kategoriler": common["Category"],
                "Etiketler": common["Tags"],
                "Resim URL": images,
                "Metadata Başlık": common["SEO Title"],
                "Metadata Açıklama": common["SEO Description"],
                "Slug": handle,
                "Stok:Ana Depo": stock_qty,
                "Tip": common["Type"],
                "Varyant Tip 1": "",  # Basit ürün için boş
                "Varyant Değer 1": "",  # Basit ürün için boş
                "Varyant Tip 2": "",  # Basit ürün için boş
                "Varyant Değer 2": "",  # Basit ürün için boş
                "Desi": "",  # Boş
                "HS Kod": "",  # Boş
                "Birim Ürün Miktarı": "",  # Boş
                "Ürün Birimi": "",  # Boş
                "Satılan Ürün Miktarı": "",  # Boş
                "Satılan Ürün Birimi": "",  # Boş
                "Google Ürün Kategorisi": common["Google Category"],
                "Tedarikçi": common["Vendor"],
                "Stoğu Tükenince Satmaya Devam Et": "",  # Boş
                "Satış Kanalı:belix": satis_kanali,  # Status/Published "Active" ise "VISIBLE"
                "Sepet Başına Minimum Alma Adeti:belix": "",  # Boş
                "Sepet Başına Maksimum Alma Adeti:belix": "",  # Boş
                "Varyant Aktiflik": "",  # Boş bırak
                "Oluşturulma Tarihi": common["Created At"],
            }
            
            ikas_rows.append(ikas_row)
            continue  # Basit ürün için döngüyü atla, zaten tek satır oluşturduk

        # VARYANTLI ÜRÜN: Aynı varyant kombinasyonuna sahip satırları birleştir
        # Sorun: Shopify'da aynı varyant için birden fazla görsel varsa, her görsel için ayrı satır olabilir
        # Çözüm: Aynı varyant değerlerine (Option1 Value + Option2 Value) sahip satırları tek satırda birleştir
        
        # Satış Kanalı: Handle seviyesinde Status kontrolü
        satis_kanali = "VISIBLE" if handle_status[handle] else ""
        
        # Varyant Tip'leri tüm satırlarda tekrarlamak için, önce tüm varyant tiplerini topla
        variant_type_1 = ""
        variant_type_2 = ""
        # İlk satırda Varyant Tip'leri belirle
        if len(group_df) > 0:
            first_row = group_df.iloc[0]
            if "Option1 Name" in first_row and pd.notna(first_row["Option1 Name"]):
                variant_type_1 = str(first_row["Option1 Name"])
            if "Option2 Name" in first_row and pd.notna(first_row["Option2 Name"]):
                variant_type_2 = str(first_row["Option2 Name"])

        # Varyant kombinasyonlarına göre grupla (Option1 Value + Option2 Value + Variant SKU)
        # Aynı varyant değerlerine sahip satırları tek satırda birleştir
        # ÖNEMLİ: Boş varyant değerlerine sahip satırları filtrele (basit ürün değilse)
        variant_combinations = {}
        
        for idx, row in group_df.iterrows():
            # Varyant değerlerini al (kombinasyon anahtarı olarak kullan)
            option1_value = ""
            option2_value = ""
            variant_sku = ""
            
            # Varyant değerlerini al ve temizle
            # NaN, None, boş string kontrolü
            option1_value = ""
            if "Option1 Value" in row:
                val = row["Option1 Value"]
                if pd.notna(val) and val != "" and str(val).strip() != "":
                    option1_value = str(val).strip()
            
            option2_value = ""
            if "Option2 Value" in row:
                val = row["Option2 Value"]
                if pd.notna(val) and val != "" and str(val).strip() != "":
                    option2_value = str(val).strip()
            
            variant_sku = ""
            if "Variant SKU" in row:
                val = row["Variant SKU"]
                if pd.notna(val) and val != "" and str(val).strip() != "":
                    variant_sku = str(val).strip()
            
            # "Default Title" değerlerini boş olarak kabul et (case-insensitive)
            if option1_value and str(option1_value).upper().strip() == "DEFAULT TITLE":
                option1_value = ""
            if option2_value and str(option2_value).upper().strip() == "DEFAULT TITLE":
                option2_value = ""
            
            # Son kontrol: Boş string'leri temizle
            if not option1_value or option1_value == "":
                option1_value = ""
            if not option2_value or option2_value == "":
                option2_value = ""
            if not variant_sku or variant_sku == "":
                variant_sku = ""
            
            # ÖNEMLİ: Geçerli varyant kontrolü ve birleştirme anahtarı
            # Varyant kombinasyon anahtarı oluştur
            # Strateji: Hem Option Value'ları hem Variant SKU'yu birlikte kullan
            # Eğer Option Value'lar varsa, onları kullan (daha güvenilir)
            # Eğer Option Value'lar yoksa ama Variant SKU varsa, onu kullan
            
            # Geçerli varyant kontrolü: En az bir Option Value veya Variant SKU olmalı
            if not option1_value and not option2_value and not variant_sku:
                # Hiç geçerli varyant bilgisi yoksa, bu satırı atla
                continue
            
            # Birleştirme anahtarı: Option Value'lar öncelikli, yoksa Variant SKU
            # ÖNEMLİ: Option Value'ları normalize et (büyük/küçük harf duyarsız, whitespace temizle)
            # Aynı varyant değeri farklı formatlarda olabilir (örn: "S" vs "S " vs "s")
            normalized_option1 = option1_value.strip().upper() if option1_value else ""
            normalized_option2 = option2_value.strip().upper() if option2_value else ""
            normalized_sku = variant_sku.strip() if variant_sku else ""
            
            if normalized_option1 or normalized_option2:
                # Option Value'lar varsa, normalize edilmiş değerleri kullan
                variant_key = (normalized_option1, normalized_option2)
            elif normalized_sku:
                # Option Value'lar yoksa ama Variant SKU varsa, onu kullan
                variant_key = ("SKU_ONLY", normalized_sku)
            else:
                # Bu duruma düşmemeli (yukarıdaki kontrol ile atlanmalı)
                continue
            
            # Bu kombinasyon için ilk kez karşılaşıyorsak, yeni bir kayıt oluştur
            if variant_key not in variant_combinations:
                # Orijinal değerleri sakla (ilk satırdan orijinal formatı koru)
                # Normalize edilmiş değerler eşleştirme için, orijinal değerler kayıt için
                variant_combinations[variant_key] = {
                    "Option1 Value": option1_value,  # Orijinal değer (normalize edilmeden önce)
                    "Option2 Value": option2_value,  # Orijinal değer (normalize edilmeden önce)
                    "rows": []  # Bu varyant için tüm satırları sakla
                }
            
            # Bu satırı bu varyant kombinasyonuna ekle
            variant_combinations[variant_key]["rows"].append(row)
        
        # Her varyant kombinasyonu için tek bir ikas satırı oluştur
        # ÖNEMLİ: Eğer hiç geçerli varyant kombinasyonu yoksa, basit ürün olarak işle
        if not variant_combinations:
            # Hiç geçerli varyant yoksa, basit ürün olarak işle
            is_simple_product = True
            grup_id = ""
            
            # Basit ürün için tüm satırları birleştir (daha önceki basit ürün mantığı)
            variant_sku = ""
            barcode = ""
            sale_price = 0.0
            discounted_price = 0.0
            stock_qty = 0
            
            for idx, row in group_df.iterrows():
                if not variant_sku and "Variant SKU" in row and pd.notna(row["Variant SKU"]):
                    variant_sku = str(row["Variant SKU"])
                if not barcode:
                    if "Variant Barcode" in row and pd.notna(row["Variant Barcode"]):
                        barcode = str(row["Variant Barcode"])
                    elif "Barcode" in row and pd.notna(row["Barcode"]):
                        barcode = str(row["Barcode"])
                if sale_price == 0.0:
                    if "Variant Price" in row and pd.notna(row["Variant Price"]):
                        try:
                            sale_price = float(row["Variant Price"])
                        except (ValueError, TypeError):
                            pass
                if discounted_price == 0.0:
                    compare_price = row.get("Compare At Price", None)
                    if compare_price is None or pd.isna(compare_price):
                        compare_price = row.get("Variant Compare At Price", None)
                    if compare_price is not None and pd.notna(compare_price):
                        try:
                            discounted_price = float(compare_price)
                        except (ValueError, TypeError):
                            pass
                if stock_qty == 0:
                    if "Variant Inventory Qty" in row and pd.notna(row["Variant Inventory Qty"]):
                        try:
                            stock_qty = int(row["Variant Inventory Qty"])
                        except (ValueError, TypeError):
                            pass
            
            satis_kanali = "VISIBLE" if handle_status[handle] else ""
            
            ikas_row = {
                "Ürün Grup ID": "",
                "Varyant ID": "",
                "İsim": common["Title"],
                "Açıklama": common["Body (HTML)"],
                "Satış Fiyatı": sale_price,
                "İndirimli Fiyatı": discounted_price,
                "Alış Fiyatı": "",
                "Barkod Listesi": barcode,
                "SKU": variant_sku,
                "Silindi mi?": "",
                "Marka": common["Vendor"],
                "Kategoriler": common["Category"],
                "Etiketler": common["Tags"],
                "Resim URL": images,
                "Metadata Başlık": common["SEO Title"],
                "Metadata Açıklama": common["SEO Description"],
                "Slug": handle,
                "Stok:Ana Depo": stock_qty,
                "Tip": common["Type"],
                "Varyant Tip 1": "",
                "Varyant Değer 1": "",
                "Varyant Tip 2": "",
                "Varyant Değer 2": "",
                "Desi": "",
                "HS Kod": "",
                "Birim Ürün Miktarı": "",
                "Ürün Birimi": "",
                "Satılan Ürün Miktarı": "",
                "Satılan Ürün Birimi": "",
                "Google Ürün Kategorisi": common["Google Category"],
                "Tedarikçi": common["Vendor"],
                "Stoğu Tükenince Satmaya Devam Et": "",
                "Satış Kanalı:belix": satis_kanali,
                "Sepet Başına Minimum Alma Adeti:belix": "",
                "Sepet Başına Maksimum Alma Adeti:belix": "",
                "Varyant Aktiflik": "",
                "Oluşturulma Tarihi": common["Created At"],
            }
            
            ikas_rows.append(ikas_row)
            continue  # Basit ürün için döngüyü atla
        
        # Varyantlı ürünler için her kombinasyonu işle
        for variant_key, variant_data in variant_combinations.items():
            variant_rows = variant_data["rows"]
            option1_value = variant_data["Option1 Value"]
            option2_value = variant_data["Option2 Value"]
            
            # İlk boş olmayan değerleri al (tüm satırlardan)
            variant_sku = ""
            barcode = ""
            sale_price = 0.0
            discounted_price = 0.0
            stock_qty = 0
            
            for row in variant_rows:
                # Varyant SKU - ilk boş olmayan değeri al
                if not variant_sku and "Variant SKU" in row and pd.notna(row["Variant SKU"]):
                    variant_sku = str(row["Variant SKU"]).strip()
                    if not variant_sku:  # Boş string ise devam et
                        continue
                
                # Barkod Listesi - ilk boş olmayan değeri al
                if not barcode:
                    if "Variant Barcode" in row and pd.notna(row["Variant Barcode"]):
                        barcode = str(row["Variant Barcode"])
                    elif "Barcode" in row and pd.notna(row["Barcode"]):
                        barcode = str(row["Barcode"])
                
                # Satış Fiyatı - ilk boş olmayan değeri al
                if sale_price == 0.0:
                    if "Variant Price" in row and pd.notna(row["Variant Price"]):
                        try:
                            sale_price = float(row["Variant Price"])
                        except (ValueError, TypeError):
                            pass
                
                # İndirimli Fiyatı - ilk boş olmayan değeri al
                if discounted_price == 0.0:
                    compare_price = row.get("Compare At Price", None)
                    if compare_price is None or pd.isna(compare_price):
                        compare_price = row.get("Variant Compare At Price", None)
                    if compare_price is not None and pd.notna(compare_price):
                        try:
                            discounted_price = float(compare_price)
                        except (ValueError, TypeError):
                            pass
                
                # Stok:Ana Depo - ilk boş olmayan değeri al
                if stock_qty == 0:
                    if "Variant Inventory Qty" in row and pd.notna(row["Variant Inventory Qty"]):
                        try:
                            stock_qty = int(row["Variant Inventory Qty"])
                        except (ValueError, TypeError):
                            pass
            
            # Varyant Tip ve Değerler - Varyantlı ürünler için
            # Varyant Tip'ler HER satırda tekrarlanarak dolu gelmeli
            variant_tip_1 = variant_type_1  # Tüm satırlarda aynı
            variant_deger_1 = ""
            variant_tip_2 = variant_type_2  # Tüm satırlarda aynı
            variant_deger_2 = ""
            
            # Varyant Değerleri - variant_data'dan al (zaten orijinal değerler saklanmış)
            # Orijinal değerleri kullan (normalize edilmiş değerler sadece eşleştirme için)
            if option1_value and option1_value.upper() != "DEFAULT TITLE":
                variant_deger_1 = option1_value
            
            if option2_value and option2_value.upper() != "DEFAULT TITLE":
                variant_deger_2 = option2_value

            # Varyant Aktiflik: Boş bırak (yeni kural)
            variant_aktiflik = ""

            # Tedarikçi (Vendor'dan veya ayrı bir sütundan)
            tedarikci = common["Vendor"]

            # ikas satırı oluştur (tüm 37 sütun)
            ikas_row = {
                "Ürün Grup ID": grup_id,
                "Varyant ID": "",  # Boş bırak
                "İsim": common["Title"],
                "Açıklama": common["Body (HTML)"],
                "Satış Fiyatı": sale_price,
                "İndirimli Fiyatı": discounted_price,
                "Alış Fiyatı": "",  # Boş
                "Barkod Listesi": barcode,
                "SKU": variant_sku,
                "Silindi mi?": "",  # Boş
                "Marka": common["Vendor"],
                "Kategoriler": common["Category"],
                "Etiketler": common["Tags"],
                "Resim URL": images,
                "Metadata Başlık": common["SEO Title"],
                "Metadata Açıklama": common["SEO Description"],
                "Slug": handle,
                "Stok:Ana Depo": stock_qty,
                "Tip": common["Type"],
                "Varyant Tip 1": variant_tip_1,
                "Varyant Değer 1": variant_deger_1,
                "Varyant Tip 2": variant_tip_2,
                "Varyant Değer 2": variant_deger_2,
                "Desi": "",  # Boş
                "HS Kod": "",  # Boş
                "Birim Ürün Miktarı": "",  # Boş
                "Ürün Birimi": "",  # Boş
                "Satılan Ürün Miktarı": "",  # Boş
                "Satılan Ürün Birimi": "",  # Boş
                "Google Ürün Kategorisi": common["Google Category"],
                "Tedarikçi": tedarikci,
                "Stoğu Tükenince Satmaya Devam Et": "",  # Boş
                "Satış Kanalı:belix": satis_kanali,  # Handle seviyesinde Status kontrolü - TÜM satırlara yazılır
                "Sepet Başına Minimum Alma Adeti:belix": "",  # Boş
                "Sepet Başına Maksimum Alma Adeti:belix": "",  # Boş
                "Varyant Aktiflik": variant_aktiflik,  # Boş bırak
                "Oluşturulma Tarihi": common["Created At"],
            }

            ikas_rows.append(ikas_row)

    # DataFrame oluştur
    ikas_df = pd.DataFrame(ikas_rows, columns=IKAS_COLUMNS)

    # NOT: Basit ürünler zaten tek satırda birleştirilmiş durumda
    # Varyantlı olmayan ürünler için ek kontrol yapılmasına gerek yok

    return ikas_df


if __name__ == "__main__":
    # Test için örnek Shopify verisi
    sample_shopify_data = pd.DataFrame(
        {
            "Handle": ["cotton-tshirt", "cotton-tshirt", "cotton-tshirt", "linen-shirt"],
            "Title": ["Cotton T-Shirt", "Cotton T-Shirt", "Cotton T-Shirt", "Linen Shirt"],
            "Body (HTML)": ["<p>Soft cotton tee</p>", "<p>Soft cotton tee</p>", "<p>Soft cotton tee</p>", "<p>Breathable linen shirt</p>"],
            "Vendor": ["ComfortWear", "ComfortWear", "ComfortWear", "BreezeLine"],
            "Type": ["Tops", "Tops", "Tops", "Tops"],
            "Product Category": ["T-Shirts", "T-Shirts", "T-Shirts", "Shirts"],
            "Tags": ["casual, summer", "casual, summer", "casual, summer", "formal, summer"],
            "Published": ["TRUE", "TRUE", "TRUE", "TRUE"],
            "Option1 Name": ["Size", "Size", "Size", ""],
            "Option1 Value": ["S", "M", "L", ""],
            "Option2 Name": ["Color", "Color", "Color", ""],
            "Option2 Value": ["Blue", "Blue", "Red", ""],
            "Variant SKU": ["CW-TS-001-S", "CW-TS-001-M", "CW-TS-001-L", "BL-LS-002"],
            "Variant Barcode": ["1234567890123", "1234567890124", "1234567890125", "9876543210987"],
            "Variant Price": [199.90, 199.90, 199.90, 349.00],
            "Compare At Price": [249.90, 249.90, 249.90, 399.00],
            "Variant Inventory Qty": [10, 25, 15, 12],
            "SEO Title": ["Cotton T-Shirt", "Cotton T-Shirt", "Cotton T-Shirt", "Linen Shirt"],
            "SEO Description": ["Soft cotton t-shirt", "Soft cotton t-shirt", "Soft cotton t-shirt", "Breathable linen shirt"],
            "Created At": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-15"],
            "Google Shopping / Google Product Category": ["Apparel & Accessories > Clothing > Shirts & Tops", "Apparel & Accessories > Clothing > Shirts & Tops", "Apparel & Accessories > Clothing > Shirts & Tops", "Apparel & Accessories > Clothing > Shirts & Tops"],
            "Image Src": [
                "https://cdn.example.com/products/cotton-tshirt-1.jpg",
                "https://cdn.example.com/products/cotton-tshirt-2.jpg",
                "",
                "https://cdn.example.com/products/linen-shirt.jpg",
            ],
            "Variant Image": [
                "",
                "https://cdn.example.com/products/cotton-tshirt-variant.jpg",
                "",
                "",
            ],
        }
    )

    # Geçici CSV dosyası oluştur
    temp_input_path = pathlib.Path("sample_shopify_export.csv")
    sample_shopify_data.to_csv(temp_input_path, index=False)

    # Dönüşümü çalıştır
    converted_df = shopify_to_ikas_converter(str(temp_input_path))
    print("Converted ikas DataFrame:\n", converted_df)
    print("\nSütunlar:", converted_df.columns.tolist())

    # Temizlik
    temp_input_path.unlink(missing_ok=True)
