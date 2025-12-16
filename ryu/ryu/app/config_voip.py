#!/usr/bin/python3

# ---------------------- KONFIGURASI DB & KOLEKSI ----------------------
DB_CONN = "dbname=development user=dev_one password=hijack332. host=127.0.0.1"
COLLECT_INTERVAL = 3 # Detik

# ---------------------- DAFTAR HOST YANG DIMONITOR ----------------------
HOSTS_TO_MONITOR = [
    'user1', 'user2', 'user3', 
    'web1', 'web2', 'cache1',
    'app1', 'db1'
]

# ---------------------- METADATA HOST (VOIP PROFILE) ----------------------
# Kita asumsikan semua node sekarang berpartisipasi dalam VoIP mesh/pairs.
# dpid: ID switch terdekat (1=Ext, 2=Rack1, 3=Rack2)
# tp_src/dst: Transport Port simulasi (5060 SIP / 10000+ RTP)
HOST_INFO = {
    # --- Clients (External) ---
    'user1': { 
        'ip': '192.168.100.1', 'mac': '00:00:00:00:01:01', 
        'dst_ip': '10.10.1.1', 'dst_mac': '00:00:00:00:0A:01', # Calls Web1
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10000, 
        'label': 'voip_external_to_dc'
    },
    'user2': { 
        'ip': '192.168.100.2', 'mac': '00:00:00:00:01:02', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01', # Calls App1
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10002, 
        'label': 'voip_external_to_dc'
    },
    'user3': { 
        'ip': '192.168.100.3', 'mac': '00:00:00:00:01:03', 
        'dst_ip': '10.10.1.2', 'dst_mac': '00:00:00:00:0A:02', # Calls Web2
        'dpid': 1, 'proto': 17, 'tp_src': 5060, 'tp_dst': 10004, 
        'label': 'voip_external_to_dc'
    },

    # --- Rack 1 Servers (Bertindak sebagai VoIP Gateways/Clients) ---
    'web1': { 
        'ip': '10.10.1.1', 'mac': '00:00:00:00:0A:01', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01', # Calls App1 (East-West)
        'dpid': 2, 'proto': 17, 'tp_src': 10000, 'tp_dst': 10002, 
        'label': 'voip_east_west'
    },
    'web2': { 
        'ip': '10.10.1.2', 'mac': '00:00:00:00:0A:02', 
        'dst_ip': '10.10.1.3', 'dst_mac': '00:00:00:00:0A:03', # Calls Cache1
        'dpid': 2, 'proto': 17, 'tp_src': 10004, 'tp_dst': 10006, 
        'label': 'voip_intra_rack'
    },
    'cache1': { 
        'ip': '10.10.1.3', 'mac': '00:00:00:00:0A:03', 
        'dst_ip': '192.168.100.1', 'dst_mac': '00:00:00:00:01:01', # Return traffic
        'dpid': 2, 'proto': 17, 'tp_src': 10006, 'tp_dst': 5060, 
        'label': 'voip_response'
    },
    
    # --- Rack 2 Servers ---
    'app1': { 
        'ip': '10.10.2.1', 'mac': '00:00:00:00:0B:01', 
        'dst_ip': '10.10.2.2', 'dst_mac': '00:00:00:00:0B:02', # Calls DB1
        'dpid': 3, 'proto': 17, 'tp_src': 10002, 'tp_dst': 10008, 
        'label': 'voip_intra_rack'
    },
    'db1':  { 
        'ip': '10.10.2.2', 'mac': '00:00:00:00:0B:02', 
        'dst_ip': '10.10.2.1', 'dst_mac': '00:00:00:00:0B:01', # Return traffic
        'dpid': 3, 'proto': 17, 'tp_src': 10008, 'tp_dst': 10002, 
        'label': 'voip_response'
    },
}