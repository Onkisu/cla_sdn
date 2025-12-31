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

echo -e "${GREEN}[1/7]${NC} Updating package lists..."
apt-get update -qq

echo -e "${GREEN}[2/7]${NC} Installing Python3 and pip..."
apt-get install -y python3 python3-pip python3-dev > /dev/null 2>&1

echo -e "${GREEN}[3/7]${NC} Installing Mininet..."
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

echo -e "${GREEN}[4/7]${NC} Installing D-ITG (Traffic Generator)..."
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

echo -e "${GREEN}[5/7]${NC} Checking Python virtual environment for Ryu..."

RYU_VENV="/home/takemi/ryu-env"

if [ -d "$RYU_VENV" ]; then
    echo "       Virtual environment exists at $RYU_VENV"
    echo "       Checking Ryu installation..."
    
    source $RYU_VENV/bin/activate
    if command -v ryu-manager &> /dev/null; then
        echo -e "       ${GREEN}✓${NC} Ryu is installed in venv"
    else
        echo "       Installing Ryu in existing venv..."
        pip install --quiet ryu eventlet
        if [ $? -eq 0 ]; then
            echo -e "       ${GREEN}✓${NC} Ryu installed"
        else
            echo -e "       ${RED}✗${NC} Failed to install Ryu"
        fi
    fi
    deactivate
else
    echo -e "       ${YELLOW}⚠${NC} Virtual environment not found at $RYU_VENV"
    echo "       Please create venv first:"
    echo "         python3 -m venv $RYU_VENV"
    echo "         source $RYU_VENV/bin/activate"
    echo "         pip install ryu eventlet"
fi

echo -e "${GREEN}[6/7]${NC} Installing other Python dependencies (no venv)..."
pip3 install --quiet --upgrade pip
pip3 install --quiet psycopg2-binary
if [ $? -eq 0 ]; then
    echo -e "       ${GREEN}✓${NC} Python dependencies installed"
else
    echo -e "       ${RED}✗${NC} Some Python packages failed to install"
fi

echo -e "${GREEN}[7/7]${NC} Setting up permissions..."
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
echo "  ✓ Ryu SDN Controller (in venv: /home/takemi/ryu-env)"
echo "  ✓ Python dependencies (psycopg2)"
if command -v ITGSend &> /dev/null; then
    echo "  ✓ D-ITG Traffic Generator"
else
    echo "  ⚠ D-ITG (optional - simulation will work without it)"
fi
echo ""
echo "Virtual Environment:"
echo "  📁 Ryu venv location: /home/takemi/ryu-env"
echo "  📂 Simulation files: /home/takemi/cla_sdn/ryu/ryu/app/files"
echo "  ℹ️  Start script will auto-activate venv for Ryu"
echo ""
echo "Next steps:"
echo "  1. Go to simulation directory:"
echo "     cd /home/takemi/cla_sdn/ryu/ryu/app/files"
echo ""
echo "  2. Test database connection:"
echo "     python3 verify_database.py"
echo ""
echo "  3. Run simulation (auto-activates venv):"
echo "     sudo ./start_simulation.sh"
echo ""
echo "  4. Manual Ryu start (if needed):"
echo "     source /home/takemi/ryu-env/bin/activate"
echo "     ryu-manager --observe-links ryu_voip_controller.py"
echo ""
echo "================================================================"
