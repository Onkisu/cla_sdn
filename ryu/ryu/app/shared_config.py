#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# [UPDATE] Daftar host yang akan dimonitor
# Diambil dari nama host di SpineLeafTopo
HOSTS_TO_MONITOR = ['h1L1', 'h2L1', 'h1L2', 'h2L2']

# [UPDATE] Mapping Aplikasi ke Host (Data Center)
# Ini adalah "glue" untuk collector.py
HOST_INFO = {
    # Host di Leaf 1 (Rack 1)
    'h1L1': {
        'ip': '10.0.1.1', 
        'app': 'Web Client', # Berperan sbg klien 'Mice Flow'
        'mac': '00:00:00:00:01:01', 
        'server_ip': '10.0.2.1', # Targetnya adalah Web Server (h1L2)
        'server_mac': '00:00:00:00:02:01'
    },
    'h2L1': {
        'ip': '10.0.1.2', 
        'app': 'API/DB Server', # Menerima 'Mice Flow' & mengirim 'Elephant Flow'
        'mac': '00:00:00:00:01:02', 
        'server_ip': '10.0.2.2', # Targetnya adalah Backup Server (h2L2)
        'server_mac': '00:00:00:00:02:02'
    },
    
    # Host di Leaf 2 (Rack 2)
    'h1L2': {
        'ip': '10.0.2.1', 
        'app': 'Web Server',  # Menerima 'Mice Flow' & mengirim 'Mice Flow'
        'mac': '00:00:00:00:02:01', 
        'server_ip': '10.0.1.2', # Targetnya adalah API Server (h2L1)
        'server_mac': '00:00:00:00:01:02'
    },
    'h2L2': {
        'ip': '10.0.2.2', 
        'app': 'Backup Server', # Menerima 'Elephant Flow'
        'mac': '00:00:00:00:02:02', 
        'server_ip': None, # Tidak punya target (hanya menerima)
        'server_mac': None
    }
}

# [UPDATE] Mapping kategori baru
APP_TO_CATEGORY = { 
    'Web Client': 'client',
    'API/DB Server': 'database',
    'Web Server': 'web',
    'Backup Server': 'storage' 
}