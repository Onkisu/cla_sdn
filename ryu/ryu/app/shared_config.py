#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 5 # Detik

# ---------------------- DAFTAR HOST YANG DIMONITOR ----------------------
# PENTING: voip1 dan voip2 harus ada di sini!
HOSTS_TO_MONITOR = ['user1', 'user2', 'user3', 'voip1', 'voip2']

# ---------------------- METADATA HOST ----------------------
# Mapping IP, MAC, dan kategori aplikasi untuk setiap host
HOST_INFO = {
    # --- User Normal ---
    'user1': {
        'ip': '192.168.100.1', 
        'app': 'web_service_https', 
        'proto': 'tcp',
        'mac': '00:00:00:00:01:01', 
        'server_ip': '10.10.1.1',
        'server_mac': '00:00:00:00:0A:01' 
    },
    'user2': {
        'ip': '192.168.100.2', 
        'app': 'web_service_http', 
        'proto': 'tcp',
        'mac': '00:00:00:00:01:02', 
        'server_ip': '10.10.1.2',
        'server_mac': '00:00:00:00:0A:02'
    },
    'user3': {
        'ip': '192.168.100.3', 
        'app': 'web_browsing_light', 
        'proto': 'tcp',
        'mac': '00:00:00:00:01:03', 
        'server_ip': '10.10.1.2',
        'server_mac': '00:00:00:00:0A:02'
    },

    # --- VoIP Clients (WAJIB DITAMBAHKAN) ---
    'voip1': {
        'ip': '192.168.10.11', 
        'app': 'voip_call_sip', 
        'proto': 'udp',            # VoIP biasanya UDP
        'mac': '00:00:00:00:10:11', 
        'server_ip': '10.10.1.1',  # Terhubung ke web1 (SIP Server 1)
        'server_mac': '00:00:00:00:0A:01'
    },
    'voip2': {
        'ip': '192.168.10.12', 
        'app': 'voip_call_sip', 
        'proto': 'udp',
        'mac': '00:00:00:00:10:12', 
        'server_ip': '10.10.2.1',  # Terhubung ke app1 (SIP Server 2)
        'server_mac': '00:00:00:00:0B:01'
    },
    
    # --- East-West Traffic (Opsional jika mau dimonitor) ---
    'web1': { 'ip': '10.10.1.1', 'app': 'ew_web_to_app', 'proto': 'tcp', 'mac': '00:00:00:00:0A:01', 'server_ip': '10.10.2.1', 'server_mac': '00:00:00:00:0B:01' },
    'web2': { 'ip': '10.10.1.2', 'app': 'ew_web_to_cache', 'proto': 'tcp', 'mac': '00:00:00:00:0A:02', 'server_ip': '10.10.1.3', 'server_mac': '00:00:00:00:0A:03' },
    'app1': { 'ip': '10.10.2.1', 'app': 'ew_app_to_db', 'proto': 'tcp', 'mac': '00:00:00:00:0B:01', 'server_ip': '10.10.2.2', 'server_mac': '00:00:00:00:0B:02' },
}

# Mapping kategori untuk log
APP_TO_CATEGORY = {
    'web_service_https': 'web',
    'web_service_http': 'web',
    'web_browsing_light': 'web',
    'voip_call_sip': 'voip',
    'ew_web_to_app': 'east_west',
    'ew_web_to_cache': 'cache',
    'ew_app_to_db': 'db'
}