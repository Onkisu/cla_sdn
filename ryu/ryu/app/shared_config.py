#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# Daftar host (klien eksternal) yang akan dimonitor oleh collector.py
HOSTS_TO_MONITOR = ['user1', 'user2', 'user3']

# Mapping Aplikasi ke Host (untuk klasifikasi lalu lintas North-South)
HOST_INFO = {
    'user1': {
        'ip': '192.168.100.1', 
        'app': 'web_service_https', 
        'mac': '00:00:00:00:01:01', 
        'server_ip': '10.10.1.1', # <-- Server web1
        'server_mac': '00:00:00:00:0A:01' 
    },
    'user2': {
        'ip': '192.168.100.2', 
        'app': 'web_service_http', 
        'mac': '00:00:00:00:01:02', 
        'server_ip': '10.10.1.2', # <-- Server web2
        'server_mac': '00:00:00:00:0A:02'
    },
    'user3': {
        'ip': '192.168.100.3', 
        'app': 'api_service',  
        'mac': '00:00:00:00:01:03', 
        'server_ip': '10.10.2.1', # <-- Server app1
        'server_mac': '00:00:00:00:0B:01'
    }
}

# Mapping kategori aplikasi
APP_TO_CATEGORY = { 
    'web_service_https': 'web_app',
    'web_service_http': 'web_app',
    'api_service': 'backend_api' 
}