#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 1 # UBAH KE 1 DETIK (Agar burst 5 detik terdeteksi)

# ---------------------- DAFTAR HOST YANG DIMONITOR ----------------------
HOSTS_TO_MONITOR = [
    'user1', 'user2', 'user3', 
    'web1', 'web2', 'cache1',
    'app1', 'db1'
]

# ---------------------- METADATA HOST (VOIP PROFILE) ----------------------
HOST_INFO = {
    # --- Clients (External) ---
    'user1': { 
        'ip': '192.168.100.1', 'mac': '00:00:00:00:01:01', 
        'dst_ip': '10.10.1.1', 'dst_mac': '00:00:00:00:0A:01', # Target Web1
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10000, 
        'label': 'voip_external_to_dc'
    },
    'user2': { 
        'ip': '192.168.100.2', 'mac': '00:00:00:00:01:02', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01', # Target App1
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10002, 
        'label': 'voip_external_to_dc'
    },
    'user3': { 
        'ip': '192.168.100.3', 'mac': '00:00:00:00:01:03', 
        'dst_ip': '10.10.1.2', 'dst_mac': '00:00:00:00:0A:02', # Target Web2
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10004, 
        'label': 'voip_external_to_dc'
    },

    # --- Server Responses / East-West ---
    'web1': { 
        'ip': '10.10.1.1', 'mac': '00:00:00:00:0A:01', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01', 
        'dpid': 2, 'proto': 17, 'tp_src': 10000, 'tp_dst': 10002, 
        'label': 'voip_east_west'
    },
    'web2': { 
        'ip': '10.10.1.2', 'mac': '00:00:00:00:0A:02', 
        'dst_ip': '10.10.1.3', 'dst_mac': '00:00:00:00:0A:03',
        'dpid': 2, 'proto': 17, 'tp_src': 10004, 'tp_dst': 10006, 
        'label': 'voip_intra_rack'
    },
    'cache1': { 
        'ip': '10.10.1.3', 'mac': '00:00:00:00:0A:03', 
        'dst_ip': '192.168.100.1', 'dst_mac': '00:00:00:00:01:01',
        'dpid': 2, 'proto': 17, 'tp_src': 10006, 'tp_dst': 5060, 
        'label': 'voip_response'
    },
    'app1': { 
        'ip': '10.10.2.1', 'mac': '00:00:00:00:0B:01', 
        'dst_ip': '10.10.2.2', 'dst_mac': '00:00:00:00:0B:02',
        'dpid': 3, 'proto': 17, 'tp_src': 10002, 'tp_dst': 10008, 
        'label': 'voip_intra_rack'
    },
    'db1':  { 
        'ip': '10.10.2.2', 'mac': '00:00:00:00:0B:02', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01',
        'dpid': 3, 'proto': 17, 'tp_src': 10008, 'tp_dst': 10002, 
        'label': 'voip_response'
    },
}