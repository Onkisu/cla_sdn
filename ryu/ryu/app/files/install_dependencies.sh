#!/bin/bash
"""
Installation script for VoIP Simulation dependencies
"""

echo "================================================================"
echo "      VoIP Simulation - Installation Script"
echo "================================================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

echo -e "${GREEN}[1/6]${NC} Updating package lists..."
apt-get update -qq

echo -e "${GREEN}[2/6]${NC} Installing Python3 and pip..."
apt-get install -y python3 python3-pip python3-dev > /dev/null 2>&1

echo -e "${GREEN}[3/6]${NC} Installing Mininet..."
if command -v mn &> /dev/null; then
    echo "       Mininet already installed"
else
    apt-get install -y mininet > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "       ${GREEN}✓${NC} Mininet installed successfully"
    else
        echo -e "       ${RED}✗${NC} Failed to install Mininet"
    fi
fi

echo -e "${GREEN}[4/6]${NC} Installing D-ITG (Traffic Generator)..."
if command -v ITGSend &> /dev/null; then
    echo "       D-ITG already installed"
else
    apt-get install -y d-itg > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "       ${GREEN}✓${NC} D-ITG installed successfully"
    else
        echo -e "       ${YELLOW}⚠${NC} D-ITG not available in repos (optional)"
    fi
fi

echo -e "${GREEN}[5/6]${NC} Installing Python dependencies..."
pip3 install --quiet --upgrade pip
pip3 install --quiet -r requirements.txt
pip3 install --quiet ryu eventlet

if [ $? -eq 0 ]; then
    echo -e "       ${GREEN}✓${NC} Python dependencies installed"
else
    echo -e "       ${RED}✗${NC} Some Python packages failed to install"
fi

echo -e "${GREEN}[6/6]${NC} Setting up permissions..."
chmod +x start_simulation.sh
chmod +x spine_leaf_voip_simulation.py
chmod +x ryu_voip_controller.py
chmod +x verify_database.py

echo ""
echo "================================================================"
echo "  Installation Complete!"
echo "================================================================"
echo ""
echo "Installed components:"
echo "  ✓ Python 3 and pip"
echo "  ✓ Mininet"
echo "  ✓ Ryu SDN Controller"
echo "  ✓ Python dependencies (psycopg2, eventlet)"
if command -v ITGSend &> /dev/null; then
    echo "  ✓ D-ITG Traffic Generator"
else
    echo "  ⚠ D-ITG (optional - simulation will work without it)"
fi
echo ""
echo "Next steps:"
echo "  1. Test database connection:"
echo "     python3 verify_database.py"
echo ""
echo "  2. Run simulation:"
echo "     sudo ./start_simulation.sh"
echo ""
echo "================================================================"
