#!/bin/bash
"""
Startup script for VoIP Simulation
Runs Ryu controller and Mininet topology together
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

# Check if Ryu is installed
if ! command -v ryu-manager &> /dev/null; then
    echo -e "${YELLOW}Warning: Ryu is not installed${NC}"
    echo "Installing Ryu..."
    pip3 install ryu
fi

# Check if Mininet is installed
if ! command -v mn &> /dev/null; then
    echo -e "${RED}Error: Mininet is not installed${NC}"
    echo "Install with: sudo apt-get install mininet"
    exit 1
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

# Start Ryu controller in background
echo -e "${GREEN}[3/5]${NC} Starting Ryu SDN controller..."
echo "       Controller will listen on port 6653"

ryu-manager --observe-links ryu_voip_controller.py > /tmp/ryu_controller.log 2>&1 &
RYU_PID=$!

echo "       Ryu PID: $RYU_PID"
echo "       Log file: /tmp/ryu_controller.log"

# Wait for Ryu to start
echo -e "${GREEN}[4/5]${NC} Waiting for Ryu controller to initialize..."
sleep 5

# Check if Ryu is running
if ps -p $RYU_PID > /dev/null; then
    echo -e "       ${GREEN}✓${NC} Ryu controller is running"
else
    echo -e "       ${RED}✗${NC} Ryu controller failed to start"
    echo "       Check log: tail -f /tmp/ryu_controller.log"
    exit 1
fi

# Start Mininet simulation
echo -e "${GREEN}[5/5]${NC} Starting Mininet topology..."
echo ""
echo "================================================================"
echo "  Simulation Started!"
echo "================================================================"
echo ""
echo "Ryu Controller: Running (PID: $RYU_PID)"
echo "Log file: /tmp/ryu_controller.log"
echo ""
echo "Starting Mininet..."
echo "Type 'exit' in Mininet CLI to stop simulation"
echo ""

# Run Mininet
python3 spine_leaf_voip_simulation.py

# Cleanup after exit
echo ""
echo "================================================================"
echo "  Cleaning up..."
echo "================================================================"

# Stop Ryu controller
echo "Stopping Ryu controller..."
kill $RYU_PID 2>/dev/null

# Clean Mininet
echo "Cleaning Mininet..."
mn -c > /dev/null 2>&1

echo ""
echo -e "${GREEN}✓ Simulation stopped successfully${NC}"
echo ""
