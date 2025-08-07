import pandas as pd

# List semua file part yang ingin digabungkan
file_paths = [
    'turnbackhoax_data_part_1.xlsx',
    'turnbackhoax_data_part_2.xlsx',
    'turnbackhoax_data_part_3.xlsx',
    'turnbackhoax_data_part_4.xlsx',
    'turnbackhoax_data_part_5.xlsx'
]

# Membaca dan menggabungkan semua file
dfs = []
for file in file_paths:
    df = pd.read_excel(file)
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

# Menyimpan hasil gabungan
combined_df.to_excel('turnbackhoax_10k_raw_data_combined.xlsx', index=False)
print(f"✅ Data berhasil digabungkan! Total {len(combined_df)} baris disimpan di turnbackhoax_10k_raw_data_combined.xlsx")