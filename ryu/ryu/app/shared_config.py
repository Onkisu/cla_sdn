#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# Daftar host yang akan dimonitor oleh collector.py
# Kita tetap memonitor klien h1, h2, h3
HOSTS_TO_MONITOR = ['h1', 'h2', 'h3']

# [OPSIONAL] Mapping Aplikasi ke Host (buat di DB)
# [UPDATE] IP dan MAC klien kebetulan sama dengan topologi Fat-Tree
# [UPDATE] 'server_ip' diubah ke IP baru di subnet 10.0.0.0/24
HOST_INFO = {
    'h1': {
        'ip': '10.0.0.1', 
        'app': 'youtube', 
        'mac': '00:00:00:00:00:01', 
        'server_ip': '10.0.0.4', # <-- Server h4 (IP baru)
        'server_mac': '00:00:00:00:00:04' # <-- MAC untuk h4
    },
    'h2': {
        'ip': '10.0.0.2', 
        'app': 'netflix', 
        'mac': '00:00:00:00:00:02', 
        'server_ip': '10.0.0.5', # <-- Server h5 (IP baru)
        'server_mac': '00:00:00:00:00:05' # <-- MAC untuk h5
    },
    'h3': {
        'ip': '10.0.0.3', 
        'app': 'twitch',  
        'mac': '00:00:00:00:00:03', 
        'server_ip': '10.0.0.7', # <-- Server h7 (IP baru)
        'server_mac': '00:00:00:00:00:07' # <-- MAC untuk h7
    }
}

# Mapping kategori (tidak berubah)
APP_TO_CATEGORY = { 
    'youtube': 'video',
    'netflix': 'video',
    'twitch': 'gaming' 
}