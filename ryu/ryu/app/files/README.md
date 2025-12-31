# Spine-Leaf VoIP Traffic Simulation dengan Mininet + Ryu Controller

## Deskripsi
Script Python untuk simulasi traffic VoIP pada topologi Spine-Leaf menggunakan Mininet, Ryu SDN Controller, dan D-ITG, dengan penyimpanan data real-time ke PostgreSQL.

## Arsitektur
- **SDN Controller**: Ryu (OpenFlow 1.3) - monitoring flow statistics
- **Network Emulation**: Mininet - virtual network topology
- **Traffic Generator**: D-ITG - VoIP traffic simulation
- **Database**: PostgreSQL - real-time data storage

## Topologi
- **3 Spine Switches** (s1, s2, s3)
- **3 Leaf Switches** (l1, l2, l3)
- **6 Hosts** (h1-h6, 2 host per leaf)
- **Full mesh** antara Spine dan Leaf switches

## Fitur
1. ✅ Topologi Spine-Leaf dengan 3 spine, 3 leaf, 6 hosts
2. ✅ Simulasi VoIP traffic menggunakan D-ITG
3. ✅ Pattern bytes_tx random (13000-19800) dengan pola sine wave 1 jam
4. ✅ Data collection setiap 1 detik
5. ✅ Automatic insert ke PostgreSQL
6. ✅ Semua kolom lengkap sesuai format

## Format Data
```
timestamp, dpid, src_ip, dst_ip, src_mac, dst_mac, 
ip_proto, tp_src, tp_dst, bytes_tx, bytes_rx, pkts_tx, pkts_rx, 
duration_sec, traffic_label
```

## Karakteristik Traffic
- **Protocol**: UDP (ip_proto = 17)
- **Ports**: RTP range (16384-32767)
- **bytes_tx**: 13000-19800 bytes/second
- **Pattern**: Sine wave dengan periode 1 jam + random noise
- **Codec**: G.711 simulation (160 byte packets, 50 pps)
- **Label**: 'voip'

## Prerequisites

### Option A: Automatic Installation (Recommended)
```bash
sudo bash install_dependencies.sh
```

### Option B: Manual Installation

### 1. Install Mininet
```bash
sudo apt-get update
sudo apt-get install mininet
```

### 2. Install Ryu SDN Controller (di Virtual Environment)

**VENV sudah ada di:** `/home/takemi/ryu-env`

```bash
# Aktivasi venv yang sudah ada
source /home/takemi/ryu-env/bin/activate

# Check apakah Ryu sudah terinstall
ryu-manager --version

# Jika belum terinstall, install Ryu di dalam venv
pip install ryu eventlet

# Deactivate (akan auto-activate oleh start script)
deactivate
```

**PENTING:** 
- Ryu HARUS di venv `/home/takemi/ryu-env`
- Start script akan otomatis aktivasi venv ini
- File simulasi di: `/home/takemi/cla_sdn/ryu/ryu/app/files`

### 3. Install D-ITG (Distributed Internet Traffic Generator)
```bash
sudo apt-get install d-itg
```

### 4. Install Python Dependencies
```bash
pip3 install -r requirements.txt
```

### 4. Database Setup
Database sudah dikonfigurasi:
- Host: 103.181.142.121
- Database: development
- User: dev_one
- Password: hiroshi451.
- Table: traffic.flow_stats_

Table akan dibuat otomatis saat script pertama kali dijalankan.

## Cara Menjalankan

### Option A: Menggunakan Start Script (Recommended)
```bash
# Install dependencies dulu (jika belum)
sudo bash install_dependencies.sh

# Jalankan simulasi (otomatis start Ryu + Mininet)
sudo ./start_simulation.sh
```

### Option B: Manual (2 Terminal)

**Terminal 1: Start Ryu Controller (di venv)**
```bash
# Aktivasi venv dari path absolute
source /home/takemi/ryu-env/bin/activate

# Masuk ke directory files
cd /home/takemi/cla_sdn/ryu/ryu/app/files

# Start Ryu
ryu-manager --observe-links ryu_voip_controller.py

# Jangan close terminal ini!
```

**Terminal 2: Start Mininet (NO venv)**
```bash
# Masuk ke directory files
cd /home/takemi/cla_sdn/ryu/ryu/app/files

# Langsung jalankan (tanpa venv)
sudo python3 spine_leaf_voip_simulation.py
```

### 3. Di Mininet CLI
```
mininet> pingall    # Test konektivitas
mininet> h1 ping h2 -c 5  # Ping test
mininet> exit       # Stop simulasi
```

### 4. Monitor Logs
```bash
# Ryu controller logs
tail -f /tmp/ryu_controller.log

# Check database real-time
python3 verify_database.py
```

## Struktur Topology

```
                    Ryu Controller (OpenFlow 1.3)
                            |
                    Port 6653 (OpenFlow)
                            |
        [s1]----[s2]----[s3]    <- Spine Layer
         |  \  / | \  / |
         |   \/  |  \/  |
         |   /\  |  /\  |
         |  /  \ | /  \ |
        [l1]----[l2]----[l3]    <- Leaf Layer
        / \     / \     / \
      h1  h2  h3  h4  h5  h6    <- Host Layer
```

## 📁 FILE STRUCTURE

```
/home/takemi/
├── ryu-env/                             # Ryu virtual environment
│   ├── bin/
│   │   ├── activate                     # Aktivasi venv
│   │   └── ryu-manager                  # Ryu binary
│   └── lib/
│
└── cla_sdn/ryu/ryu/app/files/          # Simulation files
    ├── ryu_voip_controller.py          # Ryu controller (RUN in venv)
    ├── spine_leaf_voip_simulation.py   # Mininet (NO venv)
    ├── start_simulation.sh             # Auto-start script
    ├── install_dependencies.sh         # Setup script
    ├── verify_database.py              # Database check
    ├── requirements.txt                # Python deps
    └── README.md                       # Documentation
```

## Komponen Sistem

### 1. Ryu SDN Controller (`ryu_voip_controller.py`)
- Monitoring flow statistics dari semua switch
- Mengumpulkan data setiap 1 detik
- Auto-insert ke PostgreSQL
- OpenFlow 1.3 protocol
- Learning switch functionality

### 2. Mininet Topology (`spine_leaf_voip_simulation.py`)
- Membuat topologi Spine-Leaf
- Menghubungkan ke Ryu controller
- Generate VoIP traffic dengan D-ITG
- Management network lifecycle

### 3. Database Verification (`verify_database.py`)
- Test koneksi database
- Verifikasi integritas data
- Show traffic statistics
- Data validation

## Data Pattern

Script menggunakan fungsi sine wave untuk mensimulasikan pattern traffic VoIP yang realistis:

```python
bytes_tx = base + (amplitude × sin(2π × t/3600)) + noise
```

- Base: 16400 bytes
- Amplitude: ±3400 bytes
- Period: 3600 seconds (1 jam)
- Noise: ±10% random variation
- Range: 13000-19800 bytes (guaranteed)

## Monitoring Database

Untuk melihat data yang tersimpan:

```sql
-- Connect ke database
psql -h 103.181.142.121 -U dev_one -d development

-- Query data
SELECT * FROM traffic.flow_stats_ 
ORDER BY timestamp DESC 
LIMIT 10;

-- Check bytes_tx range
SELECT 
    MIN(bytes_tx) as min_bytes,
    MAX(bytes_tx) as max_bytes,
    AVG(bytes_tx) as avg_bytes,
    COUNT(*) as total_records
FROM traffic.flow_stats_;

-- Traffic by host
SELECT 
    src_ip, 
    dst_ip, 
    COUNT(*) as flow_count,
    AVG(bytes_tx) as avg_bytes_tx
FROM traffic.flow_stats_
GROUP BY src_ip, dst_ip
ORDER BY flow_count DESC;
```

## Troubleshooting

### Error: "Ryu virtual environment not found"
Check venv location:
```bash
ls -la /home/takemi/ryu-env/bin/activate

# Jika tidak ada, buat venv baru:
python3 -m venv /home/takemi/ryu-env
source /home/takemi/ryu-env/bin/activate
pip install ryu eventlet
deactivate
```

### Error: "Cannot connect to Ryu controller"
Pastikan Ryu controller running di port 6653:
```bash
# Check if Ryu is running
ps aux | grep ryu-manager

# Check port
netstat -tulpn | grep 6653

# Restart Ryu (di venv)
pkill -f ryu-manager
source /home/takemi/ryu-env/bin/activate
cd /home/takemi/cla_sdn/ryu/ryu/app/files
ryu-manager --observe-links ryu_voip_controller.py
```

### Error: "Ryu not found" (saat manual start)
Aktivasi venv:
```bash
source /home/takemi/ryu-env/bin/activate
cd /home/takemi/cla_sdn/ryu/ryu/app/files
ryu-manager --observe-links ryu_voip_controller.py
```

### Error: "D-ITG not found"
Install D-ITG:
```bash
sudo apt-get install d-itg
```
Script akan tetap berjalan dengan simulated data jika D-ITG tidak tersedia.

### Error: "Database connection failed"
- Cek koneksi internet
- Verifikasi credentials database
- Pastikan firewall mengizinkan koneksi ke port 5432
- Test dengan: `python3 verify_database.py`

### Error: "OVS switch connection failed"
Clean dan restart:
```bash
sudo mn -c
sudo systemctl restart openvswitch-switch
sudo ./start_simulation.sh
```

### Logs tidak muncul
Check Ryu logs:
```bash
tail -f /tmp/ryu_controller.log
```

## Validasi Data

Script memastikan:
1. ✅ bytes_tx selalu dalam range 13000-19800
2. ✅ Semua kolom terisi lengkap (tidak ada NULL)
3. ✅ Data diinsert setiap 1 detik
4. ✅ Pattern naik-turun dengan periode 1 jam
5. ✅ Protocol VoIP (UDP, port RTP)

## Advanced Usage

### Custom Duration
Edit di script, ubah waktu di CLI atau gunakan:
```bash
# Run for 1 hour then exit
timeout 3600 sudo python3 spine_leaf_voip_simulation.py
```

### Export Data
```bash
# Export to CSV
psql -h 103.181.142.121 -U dev_one -d development \
  -c "COPY traffic.flow_stats_ TO STDOUT CSV HEADER" > voip_data.csv
```

## Notes
- Script menggunakan threading untuk collection data paralel
- Auto-reconnect jika database connection lost
- Graceful shutdown dengan Ctrl+C
- Logs detail untuk troubleshooting

## License
MIT License

## Author
Created for VoIP traffic analysis and network simulation research.
