#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# Daftar host yang akan dimonitor oleh collector.py
HOSTS_TO_MONITOR = ['h1', 'h2', 'h3']

# [OPSIONAL] Mapping Aplikasi ke Host (buat di DB)
# [UPDATE] Ditambahkan 'server_ip' untuk target ping latensi
HOST_INFO = {
    'h1': {
        'ip': '10.0.0.1', 
        'app': 'youtube', 
        'mac': '00:00:00:00:00:01', 
        'server_ip': '10.0.1.1' # <-- Server h4
    },
    'h2': {
        'ip': '10.0.0.2', 
        'app': 'netflix', 
        'mac': '00:00:00:00:00:02', 
        'server_ip': '10.0.1.2' # <-- Server h5
    },
    'h3': {
        'ip': '10.0.0.3', 
        'app': 'twitch',  
        'mac': '00:00:00:00:00:03', 
        'server_ip': '10.0.2.1' # <-- Server h7
    }
}

# Mapping kategori (diambil dari v8.0)
APP_TO_CATEGORY = { 
    'youtube': 'video',
    'netflix': 'video',
    'twitch': 'video' 
}