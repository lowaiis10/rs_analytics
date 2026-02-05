#!/bin/bash
# ==============================================
# RS Analytics VPS Deployment Script
# ==============================================
# This script sets up the Ubuntu VPS for hosting
# the RS Analytics Streamlit dashboard.
#
# Run as root: bash setup_vps.sh
# ==============================================

set -e  # Exit on any error

echo "=========================================="
echo "RS Analytics VPS Setup Script"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ==============================================
# Step 1: System Update
# ==============================================
echo -e "${YELLOW}[1/8] Updating system packages...${NC}"
apt update && apt upgrade -y

# ==============================================
# Step 2: Install Required Packages
# ==============================================
echo -e "${YELLOW}[2/8] Installing Python and dependencies...${NC}"
apt install -y python3 python3-pip python3-venv git nginx ufw

# Verify Python version
PYTHON_VERSION=$(python3 --version)
echo -e "${GREEN}Installed: ${PYTHON_VERSION}${NC}"

# ==============================================
# Step 3: Create Application User
# ==============================================
echo -e "${YELLOW}[3/8] Creating application user...${NC}"
if id "rsanalytics" &>/dev/null; then
    echo "User 'rsanalytics' already exists"
else
    useradd -m -s /bin/bash rsanalytics
    echo "rsanalytics:rsanalytics123" | chpasswd
    usermod -aG sudo rsanalytics
    echo -e "${GREEN}Created user 'rsanalytics'${NC}"
fi

# ==============================================
# Step 4: Create Application Directory
# ==============================================
echo -e "${YELLOW}[4/8] Setting up application directory...${NC}"
APP_DIR="/home/rsanalytics/rs_analytics"
mkdir -p $APP_DIR
mkdir -p $APP_DIR/data
mkdir -p $APP_DIR/logs
mkdir -p $APP_DIR/secrets

# ==============================================
# Step 5: Create Systemd Service
# ==============================================
echo -e "${YELLOW}[5/8] Creating systemd service...${NC}"
cat > /etc/systemd/system/rsanalytics.service << 'EOF'
[Unit]
Description=RS Analytics Streamlit Dashboard
After=network.target

[Service]
Type=simple
User=rsanalytics
Group=rsanalytics
WorkingDirectory=/home/rsanalytics/rs_analytics
Environment="PATH=/home/rsanalytics/rs_analytics/venv/bin:/usr/bin"
ExecStart=/home/rsanalytics/rs_analytics/venv/bin/streamlit run app/main.py --server.port 8501 --server.address 127.0.0.1 --server.headless true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}Created systemd service${NC}"

# ==============================================
# Step 6: Configure Nginx Reverse Proxy
# ==============================================
echo -e "${YELLOW}[6/8] Configuring Nginx...${NC}"
cat > /etc/nginx/sites-available/rsanalytics << 'EOF'
server {
    listen 80;
    server_name _;

    # Main application
    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 86400;
        proxy_buffering off;
    }

    # Streamlit websocket support
    location /_stcore/stream {
        proxy_pass http://127.0.0.1:8501/_stcore/stream;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
EOF

# Enable the site
ln -sf /etc/nginx/sites-available/rsanalytics /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# Test nginx config
nginx -t
echo -e "${GREEN}Nginx configured${NC}"

# ==============================================
# Step 7: Configure Firewall
# ==============================================
echo -e "${YELLOW}[7/8] Configuring firewall...${NC}"
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw --force enable
echo -e "${GREEN}Firewall configured${NC}"

# ==============================================
# Step 8: Set Permissions
# ==============================================
echo -e "${YELLOW}[8/8] Setting permissions...${NC}"
chown -R rsanalytics:rsanalytics /home/rsanalytics/rs_analytics

# ==============================================
# Summary
# ==============================================
echo ""
echo -e "${GREEN}=========================================="
echo "VPS Setup Complete!"
echo "==========================================${NC}"
echo ""
echo "Next steps:"
echo "1. Upload your application files to: /home/rsanalytics/rs_analytics/"
echo "2. Run the app setup script: bash /home/rsanalytics/rs_analytics/deploy/setup_app.sh"
echo ""
echo "VPS Details:"
echo "  - App directory: /home/rsanalytics/rs_analytics"
echo "  - Service name: rsanalytics"
echo "  - Nginx config: /etc/nginx/sites-available/rsanalytics"
echo ""
