import os
import re

import paramiko
from dotenv import load_dotenv
from pyBiblioteca import checkFolder

load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = os.getenv("SFTP_PORT")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTPFOLDER = os.getenv("SFTPFOLDER")


def normalizar_arquivos_zip():
    pasta = f"{os.getcwd()}/sftp"

    for nome_arquivo in os.listdir(pasta):
        if not nome_arquivo.endswith('.zip'):
            continue

        base, ext = os.path.splitext(nome_arquivo)
        base_limpo = re.sub(r'(_\d+)+$', '', base)
        nome_novo = base_limpo + ext

        if nome_novo != nome_arquivo:
            caminho_original = os.path.join(pasta, nome_arquivo)
            caminho_novo = os.path.join(pasta, nome_novo)

            if not os.path.exists(caminho_novo):
                os.rename(caminho_original, caminho_novo)
                print(f"Normalizado: {nome_arquivo} -> {nome_novo}")
            else:
                os.remove(caminho_original)
                print(f"Removido duplicata: {nome_arquivo} (já existe {nome_novo})")


def readArquivo(nome_arquivo):
    with open(nome_arquivo, 'r') as arquivo:
        # Lê todas as linhas do arquivo e armazena em uma lista
        linhas = arquivo.readlines()
        # Itera sobre a lista de linhas
        for linha in linhas:
            # Imprime a linha
            print(linha.strip())

            # conectSFTP("download", linha.strip(), f"{os.getcwd()}/sftp/{linha.strip()}", f"/var/www/html/andromeda/pages/whatsapp/arquivos/ziplidos/{linha.strip()}")

            conectSFTP("download", linha.strip(), f"{os.getcwd()}/sftp/{linha.strip()}",
                       f"/var/www/html/andromeda/pages/whatsapp/arquivos/Error/{linha.strip()}")


def conectSFTP(action, filename, local_path, remote_path):
    # Crie uma instância do cliente SSH
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Conecte-se ao servidor SFTP
        ssh_client.connect(SFTP_HOST, int(SFTP_PORT), SFTP_USER, SFTP_PASS)

        # Crie uma sessão SFTP
        sftp = ssh_client.open_sftp()

        if action == 'upload':
            # Verifique se o caminho local foi fornecido
            if local_path is None:
                raise ValueError("O caminho do arquivo local deve ser fornecido para upload.")

            # Envie o arquivo
            status = sftp.put(local_path, remote_path)

            # Verifique o status da operação
            if status:
                print(f'Arquivo enviado com sucesso. Atributos do arquivo remoto: {status}')
            else:
                print(f'Erro ao enviar o arquivo.')

        elif action == 'download':
            # Verifique se o caminho local foi fornecido
            if local_path is None:
                raise ValueError("O caminho do arquivo local deve ser fornecido para download.")

            # Baixe o arquivo
            sftp.get(remote_path, local_path)
            print(f'Arquivo baixado com sucesso para {local_path}.')

        else:
            print("Ação inválida. Use 'upload' ou 'download'.")

    except Exception as e:
        print(f'Erro: {e}')

    finally:
        # Feche a sessão SFTP e a conexão SSH
        sftp.close()
        ssh_client.close()


if __name__ == '__main__':
    checkFolder(SFTPFOLDER)

    nome_arquivo = f"{os.getcwd()}/Lista.txt"

    readArquivo(nome_arquivo)

    normalizar_arquivos_zip()

# Para enviar um arquivo
# conectSFTP(action='upload', filename='meuarquivo.zip', local_path='/var/www/html/andromeda/pages/whatsapp/arquivos/ziplidos/arquivo_local.zip', remote_path='/var/www/html/andromeda/pages/whatsapp/arquivos/ziplidos/arquivo_local.zip')

# Para baixar um arquivo
# conectSFTP(action='download', filename='meuarquivo.zip', local_path='/var/www/html/andromeda/pages/whatsapp/arquivos/ziplidos/arquivo_local.zip', remote_path='/var/www/html/andromeda/pages/whatsapp/arquivos/ziplidos/arquivo_local.zip')
