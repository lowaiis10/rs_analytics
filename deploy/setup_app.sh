#!/bin/bash
# ==============================================
# RS Analytics Application Setup Script
# ==============================================
# Run this AFTER setup_vps.sh and uploading files
# Run as rsanalytics user: bash setup_app.sh
# ==============================================

set -e

echo "=========================================="
echo "RS Analytics Application Setup"
echo "=========================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

APP_DIR="/home/rsanalytics/rs_analytics"
cd $APP_DIR

# ==============================================
# Step 1: Create Virtual Environment
# ==============================================
echo -e "${YELLOW}[1/5] Creating Python virtual environment...${NC}"
python3 -m venv venv
source venv/bin/activate

# ==============================================
# Step 2: Install Dependencies
# ==============================================
echo -e "${YELLOW}[2/5] Installing Python dependencies...${NC}"
pip install --upgrade pip
pip install -r requirements.txt

# ==============================================
# Step 3: Setup Environment File
# ==============================================
echo -e "${YELLOW}[3/5] Setting up environment file...${NC}"
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo -e "${YELLOW}Created .env from .env.example${NC}"
        echo -e "${RED}IMPORTANT: Edit .env with your actual credentials!${NC}"
    else
        echo -e "${RED}Warning: No .env.example found${NC}"
    fi
else
    echo ".env already exists"
fi

# ==============================================
# Step 4: Create Data Directory Structure
# ==============================================
echo -e "${YELLOW}[4/5] Creating directory structure...${NC}"
mkdir -p data
mkdir -p logs
mkdir -p secrets

# ==============================================
# Step 5: Test Application
# ==============================================
echo -e "${YELLOW}[5/5] Testing application import...${NC}"
python3 -c "import streamlit; import duckdb; import pandas; print('All imports successful!')"

# ==============================================
# Start Services
# ==============================================
echo ""
echo -e "${GREEN}=========================================="
echo "Application Setup Complete!"
echo "==========================================${NC}"
echo ""
echo "To start the application:"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl start rsanalytics"
echo "  sudo systemctl enable rsanalytics"
echo "  sudo systemctl restart nginx"
echo ""
echo "To check status:"
echo "  sudo systemctl status rsanalytics"
echo ""
echo "To view logs:"
echo "  sudo journalctl -u rsanalytics -f"
echo ""
echo -e "${YELLOW}IMPORTANT: Don't forget to:${NC}"
echo "  1. Edit .env with your API credentials"
echo "  2. Upload your Google service account JSON to secrets/"
echo ""
