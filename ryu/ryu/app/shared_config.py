#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332 host=127.0.0.1"
COLLECT_INTERVAL = 5  # Detik

# Daftar host yang akan dimonitor
HOSTS_TO_MONITOR = ['web1', 'app1', 'db1']

# Mapping per host di data center
HOST_INFO = {
    'web1': {
        'ip': '10.0.1.10',
        'role': 'frontend',
        'mac': '00:00:00:00:01:10',
        'server_ip': '10.0.2.10',
        'server_mac': '00:00:00:00:02:10'
    },
    'app1': {
        'ip': '10.0.2.10',
        'role': 'application',
        'mac': '00:00:00:00:02:10',
        'server_ip': '10.0.3.10',
        'server_mac': '00:00:00:00:03:10'
    },
    'db1': {
        'ip': '10.0.3.10',
        'role': 'database',
        'mac': '00:00:00:00:03:10',
        'server_ip': None,
        'server_mac': None
    }
}

# Kategori layanan di level Data Center
ROLE_TO_CATEGORY = {
    'frontend': 'service-access',
    'application': 'processing',
    'database': 'storage'
}
