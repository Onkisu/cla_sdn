#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack32. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# Daftar host yang akan dimonitor oleh collector.py
# (Berdasarkan loop 'hosts_to_monitor' di v8.0)
HOSTS_TO_MONITOR = ['h1', 'h2', 'h3']

# [OPSIONAL] Mapping Aplikasi ke Host (buat di DB)
# Diperluas dengan IP dan MAC dari Topologi v8.0
HOST_INFO = {
    'h1': {'ip': '10.0.0.1', 'app': 'youtube', 'mac': '00:00:00:00:00:01'},
    'h2': {'ip': '10.0.0.2', 'app': 'netflix', 'mac': '00:00:00:00:00:02'},
    'h3': {'ip': '10.0.0.3', 'app': 'twitch',  'mac': '00:00:00:00:00:03'}
}

# Mapping kategori (diambil dari v8.0)
APP_TO_CATEGORY = { 
    'youtube': 'video',
    'netflix': 'video',
    'twitch': 'video' 
}