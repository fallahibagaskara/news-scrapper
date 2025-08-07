import pandas as pd
import glob
import csv  # Tambahan penting

def convert_xlsx_to_csv():
    # Cari semua file xlsx di folder
    xlsx_files = glob.glob('*.xlsx')
    
    if not xlsx_files:
        print("Tidak ditemukan file XLSX di direktori ini")
        return
    
    for xlsx_file in xlsx_files:
        try:
            # Baca file Excel
            df = pd.read_excel(xlsx_file)
            
            # Buat nama file CSV
            csv_file = xlsx_file.replace('.xlsx', '.csv')
            
            # Simpan ke CSV dengan semua kolom dikutip
            df.to_csv(csv_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
            print(f"Berhasil konversi: {xlsx_file} -> {csv_file}")
            
        except Exception as e:
            print(f"Gagal konversi {xlsx_file}: {str(e)}")

if __name__ == "__main__":
    print("Memulai konversi XLSX ke CSV...")
    convert_xlsx_to_csv()
    print("Proses selesai!")
