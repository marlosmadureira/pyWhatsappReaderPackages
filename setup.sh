#!/bin/bash

# Definindo variáveis

#LOCAL ONDE VAI FICAR O SISTEMA
SCRIPT_DIR="/opt/pyWhatsappReaderPackages"

# NOME DA PRINCIPAL DA APLICAÇÃO
SCRIPT_NAME="pyMain.py"

# NO DO SERVIÇO
SCRIPT_SERVICE="pyWhatsappReaderPackages"

# Cria o diretório, se não existir
mkdir -p $SCRIPT_DIR

# Instala as dependências
sudo apt install python3-pip python3-psycopg2 python3-requests -y

# Instala Bibliotecas
sudo pip3 install requests

# Dá permissão de execução ao script Python
chmod +x $SCRIPT_DIR/$SCRIPT_NAME

# Cria um serviço systemd para o script
SERVICE_FILE="/etc/systemd/system/$SCRIPT_SERVICE.service"

echo "[Unit]
Description=$SCRIPT_SERVICE Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 $SCRIPT_DIR/$SCRIPT_NAME
WorkingDirectory=$SCRIPT_DIR
StandardOutput=inherit
StandardError=inherit
Restart=always
User=root

[Install]
WantedBy=multi-user.target" | sudo tee $SERVICE_FILE

# Recarrega os serviços systemd, inicia e habilita o serviço
sudo chmod +x $SERVICE_FILE
sudo systemctl daemon-reload
sudo systemctl enable $SCRIPT_SERVICE.service
sudo systemctl start $SCRIPT_SERVICE.service

echo "Script configurado com sucesso!, Agora configure o arquivo config e depois ative e inicie o serviço: $SCRIPT_SERVICE.service"
