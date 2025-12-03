#!/usr/bin/python3

# # ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
# DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
# COLLECT_INTERVAL = 5 # Detik

# # Daftar host (klien eksternal) yang akan dimonitor oleh collector.py
# HOSTS_TO_MONITOR = ['user1', 'user2', 'user3']

# # Mapping Aplikasi ke Host (untuk klasifikasi lalu lintas North-South)
# HOST_INFO = {
#     'user1': {
#         'ip': '192.168.100.1', 
#         'app': 'web_service_https', 
#         'mac': '00:00:00:00:01:01', 
#         'server_ip': '10.10.1.1', # <-- Server web1
#         'server_mac': '00:00:00:00:0A:01' 
#     },
#     'user2': {
#         'ip': '192.168.100.2', 
#         'app': 'web_service_http', 
#         'mac': '00:00:00:00:01:02', 
#         'server_ip': '10.10.1.2', # <-- Server web2
#         'server_mac': '00:00:00:00:0A:02'
#     },
#     'user3': {
#         'ip': '192.168.100.3', 
#         'app': 'api_service',  
#         'mac': '00:00:00:00:01:03', 
#         'server_ip': '10.10.2.1', # <-- Server app1
#         'server_mac': '00:00:00:00:0B:01'
#     }
# }

# # Mapping kategori aplikasi
# APP_TO_CATEGORY = { 
#     'web_service_https': 'web_app',
#     'web_service_http': 'web_app',
#     'api_service': 'backend_api' 
# }



# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5  # Detik

# Daftar host yang akan di-collect traffic-nya
# (semua ini adalah namespace network simulasi)
HOSTS_TO_MONITOR = [
    'voip1', 'voip2',
    'web1', 'web2',
    'api1',
    'app1', 'app2',
    'db1',
    'cache1'
]

# Mapping info host → aplikasi + tujuan server (untuk North-South)
HOST_INFO = {
    # ---------------- VoIP Traffic ----------------
    'voip1': {
        'ip': '192.168.10.11',
        'app': 'voip_rtp',
        'proto': 'udp',
        'mac': '00:00:00:00:10:11',
        'server_ip': '10.10.50.1',
        'server_mac': '00:00:00:00:50:01'
    },
    'voip2': {
        'ip': '192.168.10.12',
        'app': 'voip_sip',
        'proto': 'udp',
        'mac': '00:00:00:00:10:12',
        'server_ip': '10.10.50.1',
        'server_mac': '00:00:00:00:50:01'
    },

    # ---------------- Web Traffic ----------------
    'web1': {
        'ip': '192.168.20.11',
        'app': 'web_https',
        'proto': 'tcp',
        'mac': '00:00:00:00:20:11',
        'server_ip': '10.10.1.1',
        'server_mac': '00:00:00:00:0A:01'
    },
    'web2': {
        'ip': '192.168.20.12',
        'app': 'web_http',
        'proto': 'tcp',
        'mac': '00:00:00:00:20:12',
        'server_ip': '10.10.1.2',
        'server_mac': '00:00:00:00:0A:02'
    },

    # ---------------- API Traffic ----------------
    'api1': {
        'ip': '192.168.30.11',
        'app': 'backend_api',
        'proto': 'tcp',
        'mac': '00:00:00:00:30:11',
        'server_ip': '10.10.2.1',
        'server_mac': '00:00:00:00:0B:01'
    },

    # ---------------- East-West Traffic ----------------
    'app1': {
        'ip': '172.16.1.11',
        'app': 'service_to_service',
        'proto': 'tcp',
        'mac': '00:00:00:00:40:11',
        'server_ip': '172.16.1.12',  # komunikasi internal antar service
        'server_mac': '00:00:00:00:40:12'
    },
    'app2': {
        'ip': '172.16.1.12',
        'app': 'service_to_service',
        'proto': 'tcp',
        'mac': '00:00:00:00:40:12',
        'server_ip': '172.16.1.11',
        'server_mac': '00:00:00:00:40:11'
    },

    # ---------------- DB Traffic ----------------
    'db1': {
        'ip': '10.10.3.1',
        'app': 'database_query',
        'proto': 'tcp',
        'mac': '00:00:00:00:60:01',
        'server_ip': '10.10.3.1',      # DB kadang tidak punya server partner
        'server_mac': '00:00:00:00:60:01'
    },

    # ---------------- Cache Traffic (Redis/Memcache) ----------------
    'cache1': {
        'ip': '10.10.4.1',
        'app': 'cache_access',
        'proto': 'tcp',
        'mac': '00:00:00:00:70:01',
        'server_ip': '10.10.4.1',
        'server_mac': '00:00:00:00:70:01'
    }
}

# Mapping kategori aplikasi
APP_TO_CATEGORY = {
    # VoIP
    'voip_rtp': 'voip',
    'voip_sip': 'voip',

    # Web
    'web_https': 'web',
    'web_http': 'web',

    # API
    'backend_api': 'api',

    # East-West
    'service_to_service': 'east_west',

    # Database
    'database_query': 'db',

    # Cache
    'cache_access': 'cache'
}
