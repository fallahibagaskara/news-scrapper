import pandas as pd

# Baca file XLSX
file_path = 'turnbackhoax_10k_raw_data.xlsx'
df = pd.read_excel(file_path)

# Fungsi untuk mengganti smart quotes dengan ASCII quotes
def replace_smart_quotes(text):
    if isinstance(text, str):
        return (text.replace('“', '"')
                    .replace('”', '"')
                    .replace("‘", "'")
                    .replace("’", "'"))
    return text

# Terapkan ke seluruh DataFrame
df_cleaned = df.map(replace_smart_quotes)

# Simpan ke file Excel baru
cleaned_file_path = 'turnbackhoax_10k_cleaned.xlsx'
df_cleaned.to_excel(cleaned_file_path, index=False)

cleaned_file_path
