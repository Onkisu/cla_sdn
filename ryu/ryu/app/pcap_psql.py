import psycopg2
import csv
import sys

# --- Konfigurasi Database ---
DB_CONFIG = {
    'dbname': 'development',
    'user': 'dev_one',      # Ganti dengan user psql kamu
    'password': 'hijack332.',  # Ganti dengan password psql kamu
    'host': '127.0.0.1',
    'port': '5432'
}

INPUT_FILE = 'traffic_result.txt'

def save_to_postgres():
    conn = None
    try:
        print(f"[*] Menghubungkan ke database {DB_CONFIG['dbname']}...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print(f"[*] Membaca file {INPUT_FILE}...")
        
        with open(INPUT_FILE, 'r') as f:
            # Menggunakan csv reader dengan delimiter TAB karena output TShark kita TSV
            reader = csv.reader(f, delimiter='\t')
            
            # Lewati baris pertama (Header)
            header = next(reader, None)
            
            # Query Insert
            # Perhatikan urutan kolom harus sesuai dengan urutan di file traffic_result.txt
            # Urutan dari TShark script sebelumnya: 
            # 1. time_relative -> "time"
            # 2. ip.src -> "source"
            # 3. Protocol -> protocol
            # 4. frame.len -> length
            # 5. frame.time -> "Arrival Time"
            # 6. Info -> info
            # 7. frame.number -> "No."
            # 8. ip.dst -> destination
            
            sql = """
                INSERT INTO pcap_logs 
                ("time", "source", protocol, length, "Arrival Time", info, "No.", destination) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            count = 0
            for row in reader:
                # Validasi sederhana: pastikan baris memiliki 8 kolom
                if len(row) < 8:
                    continue
                
                # Membersihkan data jika perlu (misal menghapus spasi berlebih)
                # TShark kadang memberikan multiple values dipisah koma jika ada tunneling, 
                # kita ambil yang pertama saja untuk simpelnya.
                
                time_rel = row[0]
                src = row[1]
                proto = row[2]
                length = row[3]
                arrival_time = row[4]
                info_text = row[5]
                number = row[6]
                dst = row[7]

                # Eksekusi insert
                cur.execute(sql, (time_rel, src, proto, length, arrival_time, info_text, number, dst))
                count += 1

            # Commit transaksi
            conn.commit()
            print(f"[*] SUKSES! Berhasil menyimpan {count} baris data ke database.")

    except FileNotFoundError:
        print(f"[!] Error: File {INPUT_FILE} tidak ditemukan. Jalankan script Mininet dulu.")
    except psycopg2.Error as e:
        print(f"[!] Error Database: {e}")
    except Exception as e:
        print(f"[!] Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("[*] Koneksi database ditutup.")

if __name__ == "__main__":
    save_to_postgres()