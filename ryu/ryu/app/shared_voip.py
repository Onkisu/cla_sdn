#!/usr/bin/python3
"""
[SHARED CONFIG]
Menyimpan konfigurasi Database dan Metadata Host.
Digunakan oleh Simulation dan Collector.
"""

# --- DATABASE CONFIG ---
DB_CONFIG = {
    'dbname': 'development', 
    'user': 'dev_one', 
    'password': 'hijack332.', 
    'host': '103.181.142.165',
    'table_name': 'traffic.flow_stats_' # Pastikan tabel ini ada
}

# --- HOST METADATA ---
# Definisi siapa Sender, siapa Receiver, dan tipe aplikasinya
HOST_MAP = {
    'h1': {
        'role': 'sender',
        'type': 'VoIP',
        'ip': '10.0.0.1', 'mac': '00:00:00:00:00:01',
        'target_ip': '10.0.0.3', 'target_mac': '00:00:00:00:00:03',
        'proto': 17, 'tp_src': 0, 'tp_dst': 5060, # UDP / SIP
        'interface': 'h1-eth0'
    },
    'h2': {
        'role': 'sender',
        'type': 'Background',
        'ip': '10.0.0.2', 'mac': '00:00:00:00:00:02',
        'target_ip': '10.0.0.4', 'target_mac': '00:00:00:00:00:04',
        'proto': 6, 'tp_src': 0, 'tp_dst': 5001, # TCP / Iperf
        'interface': 'h2-eth0'
    },
    'h3': {
        'role': 'receiver', 
        'type': 'VoIP_Server',
        'ip': '10.0.0.3', 'mac': '00:00:00:00:00:03',
        'interface': 'h3-eth0'
    },
    'h4': {
        'role': 'receiver',
        'type': 'Background_Server',
        'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04',
        'interface': 'h4-eth0'
    }
}