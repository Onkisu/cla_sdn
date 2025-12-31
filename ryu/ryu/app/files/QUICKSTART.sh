#!/bin/bash
# ================================================================
# QUICK START GUIDE - VoIP Simulation
# ================================================================

cat << 'EOF'

╔═══════════════════════════════════════════════════════════════════╗
║                   VoIP SIMULATION - QUICK START                   ║
╚═══════════════════════════════════════════════════════════════════╝

📦 STEP 1: INSTALASI (Sekali aja)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    cd /home/takemi/cla_sdn/ryu/ryu/app/files
    sudo bash install_dependencies.sh

    ✓ Install Mininet
    ✓ Install D-ITG
    ✓ Check venv di /home/takemi/ryu-env
    ✓ Install Ryu di venv (jika belum)
    ✓ Install psycopg2

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🧪 STEP 2: TEST DATABASE (Optional)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    cd /home/takemi/cla_sdn/ryu/ryu/app/files
    python3 verify_database.py

    ✓ Test koneksi ke PostgreSQL
    ✓ Verify table exists
    ✓ Check data integrity

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 STEP 3: JALANKAN SIMULASI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    cd /home/takemi/cla_sdn/ryu/ryu/app/files
    sudo ./start_simulation.sh

    Script ini akan OTOMATIS:
    ✓ Aktivasi venv /home/takemi/ryu-env
    ✓ Start Ryu Controller (port 6653)
    ✓ Start Mininet Topology
    ✓ Generate VoIP Traffic
    ✓ Insert data ke PostgreSQL setiap detik

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 STEP 4: DI MININET CLI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    mininet> pingall           # Test semua host
    mininet> h1 ping h2 -c 5   # Ping test
    mininet> exit              # Stop simulasi

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 MONITORING (Terminal Terpisah - Optional)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # Terminal 2: Monitor Ryu logs
    tail -f /tmp/ryu_controller.log

    # Terminal 3: Monitor database
    watch -n 2 "python3 verify_database.py"

    # Terminal 4: PostgreSQL query
    psql -h 103.181.142.121 -U dev_one -d development

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️  MANUAL START (Kalau perlu kontrol lebih)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Terminal 1: Ryu (di venv)
    ─────────────────────────
    source /home/takemi/ryu-env/bin/activate
    cd /home/takemi/cla_sdn/ryu/ryu/app/files
    ryu-manager --observe-links ryu_voip_controller.py

    Terminal 2: Mininet (NO venv)
    ──────────────────────────────
    cd /home/takemi/cla_sdn/ryu/ryu/app/files
    sudo python3 spine_leaf_voip_simulation.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 PENTING!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    ✓ Ryu HARUS di venv: /home/takemi/ryu-env
    ✓ Files di: /home/takemi/cla_sdn/ryu/ryu/app/files
    ✓ Mininet TIDAK pakai venv
    ✓ Start script OTOMATIS handle venv
    ✓ Selalu jalankan dengan: sudo ./start_simulation.sh

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📁 FILE STRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /home/takemi/
    ├── ryu-env/                          # Ryu virtual environment
    │   ├── bin/
    │   │   ├── activate                  # Aktivasi venv
    │   │   └── ryu-manager              # Ryu binary
    │   └── lib/
    └── cla_sdn/ryu/ryu/app/files/       # Simulation directory
        ├── ryu_voip_controller.py       # Ryu controller (RUN in venv)
        ├── spine_leaf_voip_simulation.py # Mininet (NO venv)
        ├── start_simulation.sh          # Auto-start script
        ├── install_dependencies.sh      # Setup script
        ├── verify_database.py           # Database check
        └── README.md                    # Full documentation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 EXPECTED OUTPUT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Setelah running, cek database:

    python3 verify_database.py

    Expected:
    ✓ Connection successful
    ✓ Table 'traffic.flow_stats_' exists
    ✓ Total records: [increasing every second]
    ✓ bytes_tx: Min ~13000, Max ~19800
    ✓ No NULL values
    ✓ Latest 5 Records shown

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 TIPS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    • Jalankan di fresh terminal (no conda/venv active)
    • Pastikan port 6653 tidak dipakai aplikasi lain
    • Kalau error, coba: sudo mn -c (clean mininet)
    • Data pattern: sine wave, 1 jam periode
    • VoIP protocol: UDP (port 16384-32767)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🆘 HELP
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Baca: README.md (full documentation)
    Logs: /tmp/ryu_controller.log

╚═══════════════════════════════════════════════════════════════════╝

EOF
