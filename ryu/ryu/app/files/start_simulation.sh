#!/bin/bash
"""
Startup script for VoIP Simulation
Runs Ryu controller (in venv) and Mininet topology (system Python) together
"""

echo "================================================================"
echo "      VoIP Traffic Simulation - Startup Script"
echo "================================================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

# Check if Ryu venv exists
RYU_VENV="/home/takemi/ryu-env"

if [ ! -d "$RYU_VENV" ]; then
    echo -e "${RED}Error: Ryu virtual environment not found at $RYU_VENV${NC}"
    echo "Please check venv location"
    exit 1
fi

# Check if Mininet is installed (system-wide for sudo)
if ! command -v mn &> /dev/null; then
    echo -e "${RED}Error: Mininet is not installed${NC}"
    echo "Install with: sudo apt-get install mininet python3-mininet"
    exit 1
fi

# Check if python3-mininet module is available (system Python)
echo "Checking Mininet Python module (system)..."
if ! python3 -c "import mininet" 2>/dev/null; then
    echo -e "${RED}Error: Mininet Python module not found${NC}"
    echo ""
    echo "Please install python3-mininet:"
    echo "  sudo apt-get update"
    echo "  sudo apt-get install python3-mininet"
    echo ""
    exit 1
else
    echo -e "  ${GREEN}✓${NC} Mininet module available"
fi

# Check if D-ITG is installed
if ! command -v ITGSend &> /dev/null; then
    echo -e "${YELLOW}Warning: D-ITG is not installed${NC}"
    echo "Install with: sudo apt-get install d-itg"
    echo "Continuing without D-ITG (will use simulated data)..."
fi

# Clean up any existing Mininet processes
echo -e "${GREEN}[1/5]${NC} Cleaning up previous Mininet processes..."
mn -c > /dev/null 2>&1

# Kill any existing Ryu processes
echo -e "${GREEN}[2/5]${NC} Stopping existing Ryu controllers..."
pkill -f ryu-manager > /dev/null 2>&1
sleep 2

# Start Ryu controller in background (IN VENV)
echo -e "${GREEN}[3/5]${NC} Starting Ryu SDN controller (in venv)..."
echo "       Venv: $RYU_VENV"
echo "       Controller will listen on port 6653"

# Run Ryu in subshell with venv activated
(
    source $RYU_VENV/bin/activate
    ryu-manager --observe-links ryu_voip_controller.py > /tmp/ryu_controller.log 2>&1 &
    echo $! > /tmp/ryu_pid.txt
)

# Get Ryu PID
sleep 1
if [ -f /tmp/ryu_pid.txt ]; then
    RYU_PID=$(cat /tmp/ryu_pid.txt)
else
    RYU_PID="unknown"
fi

echo "       Ryu PID: $RYU_PID"
echo "       Log file: /tmp/ryu_controller.log"

# Wait for Ryu to start
echo -e "${GREEN}[4/5]${NC} Waiting for Ryu controller to initialize..."
sleep 5

# Check if Ryu is running
if [ "$RYU_PID" != "unknown" ] && ps -p $RYU_PID > /dev/null 2>&1; then
    echo -e "       ${GREEN}✓${NC} Ryu controller is running"
else
    echo -e "       ${RED}✗${NC} Ryu controller failed to start"
    echo "       Check log: tail -f /tmp/ryu_controller.log"
    rm -f /tmp/ryu_pid.txt
    exit 1
fi

# Start Mininet simulation (SYSTEM PYTHON - NO VENV)
echo -e "${GREEN}[5/5]${NC} Starting Mininet topology (system Python)..."
echo ""
echo "================================================================"
echo "  Simulation Started!"
echo "================================================================"
echo ""
echo "Ryu Controller: Running in venv (PID: $RYU_PID)"
echo "Mininet: Running with system Python"
echo "Log file: /tmp/ryu_controller.log"
echo ""
echo "Starting Mininet..."
echo "Type 'exit' in Mininet CLI to stop simulation"
echo ""

# CRITICAL: Run Mininet with system Python (NO venv)
# Use absolute path to system python3
/usr/bin/python3 spine_leaf_voip_simulation.py

# Cleanup after exit
echo ""
echo "================================================================"
echo "  Cleaning up..."
echo "================================================================"

# Stop Ryu controller
echo "Stopping Ryu controller..."
if [ "$RYU_PID" != "unknown" ]; then
    kill $RYU_PID 2>/dev/null
fi
pkill -f ryu-manager 2>/dev/null

# Clean up PID file
rm -f /tmp/ryu_pid.txt

# Clean Mininet
echo "Cleaning Mininet..."
mn -c > /dev/null 2>&1

echo ""
echo -e "${GREEN}✓ Simulation stopped successfully${NC}"
echo ""
