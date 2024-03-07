import fnmatch
import json
import os
import re
import psycopg2
import requests
import time
import shutil
import zipfile

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime

# Configs
load_dotenv()

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

DebugMode = False


def clean_html(html_text):
    """Remove tags HTML e espaços extras de uma string HTML."""
    text = re.sub('<[^>]+>', '', html_text)  # Remove tags HTML
    text = re.sub('\s+', ' ', text)  # Substitui múltiplos espaços por um único espaço
    return text.strip()


def remover_espacos_regex(texto):
    return re.sub(r"\s", "", texto)


def somentenumero(parametro):
    return re.sub('[^0-9]', '', parametro)


def countdown(num_of_secs):
    while num_of_secs:
        m, s = divmod(num_of_secs, 60)
        min_sec_format = '{:02d}:{:02d}'.format(m, s)
        print(min_sec_format, end='\t')
        time.sleep(1)
        num_of_secs -= 1


def delete_log(nome_arquivo):
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)
        print_color(f"\nExcluido {nome_arquivo}", 31)


def grava_log(content, arquivo):
    arquivo = f"log/{arquivo}"
    with open(arquivo, "a") as text_file:
        text_file.write('{}\n'.format(content) + '\n')
    text_file.close()


def conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS):
    con = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return con


def print_color(text, color_code):
    print(f"\033[{color_code}m{text}\033[0m")


def openJsonEstruturado(dados_json):
    # Exibindo de forma estruturada
    json_formatado = json.dumps(dados_json, indent=2, ensure_ascii=False)

    print(f"{json_formatado}")


def checkFolder(FolderPath):
    if not os.path.exists(FolderPath):
        os.makedirs(FolderPath)


def check_internet():
    url = 'http://www.google.com/'
    timeout = 5
    try:
        _ = requests.get(url, timeout=timeout)
        return True
    except requests.ConnectionError:
        return False


def UpdateStatus():
    payload = {'token': APITOKEN, 'action': 'updateStatus'}

    try:
        r = requests.post(APILINK, data=payload)

        if r.status_code == 200:
            updateTime = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            printDebug(' Atualizado o Status em: ' + str(updateTime))
    except requests.exceptions.ConnectionError:
        print('build http connection failed')
    except Exception as inst:
        errorData = "{Location: UpdateStatus, error: " + str(inst) + "}"
        # sendSlackMSG(errorData)


def StatusServidor(dttmpstatus):
    intervalostatus = 30
    dtstatus = datetime.now().strftime('%Y%m%d%H%M%S')
    result = False

    if dttmpstatus != "":
        dtstatus = datetime.strptime(dtstatus, "%Y%m%d%H%M%S")
        anterior = datetime.strptime(dttmpstatus, "%Y%m%d%H%M%S")
        diferenca = dtstatus - anterior
        cal1 = diferenca.seconds
        cal2 = intervalostatus * 60
        if cal1 > cal2:
            result = True

    if dttmpstatus == "" or result == True:
        if check_internet():
            UpdateStatus()
            result = True
    return result


def printTimeData():
    return str(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))


def printDebug(Msg, Comment="Debug:"):
    if DebugMode:
        print(str(Comment) + " " + str(Msg))


def unzipBase(fileZIP, DIRNOVOS, DIREXTRACAO):
    OutPutDie = fileZIP.replace(".zip", "")
    zip_ref = zipfile.ZipFile(fileZIP)
    zip_ref.extractall(OutPutDie)
    zip_ref.close()

    arquivo = OutPutDie.replace(DIRNOVOS, "")
    destinationFolder = DIREXTRACAO + arquivo

    countdown(2)
    print('\n')

    if not os.path.isdir(destinationFolder):
        shutil.copytree(OutPutDie, destinationFolder)
        countdown(2)
        print('\n')

    return OutPutDie


def removeFolderFiles(FolderPath):
    if os.path.exists(FolderPath):
        shutil.rmtree(FolderPath)


def parseHTMLFile(folderZip):
    htmlFile = folderZip + "/records.html"

    soupHtml = None

    if os.path.exists(htmlFile):
        with open(htmlFile, 'r', encoding='utf-8') as f:
            contents = f.read()
            soupHtml = BeautifulSoup(contents, 'html.parser')
            soupHtml.prettify()

    return soupHtml


def contar_arquivos_zip(diretorio):
    """
    Conta quantos arquivos ZIP existem dentro de um diretório.

    Args:
    - diretorio: caminho do diretório a ser verificado.

    Returns:
    - int: número de arquivos ZIP encontrados no diretório.
    """
    # Inicializa o contador de arquivos ZIP
    contador_zip = 0

    # Lista todos os arquivos e diretórios no caminho especificado
    for arquivo in os.listdir(diretorio):
        # Verifica se o item é um arquivo e termina com '.zip'
        if os.path.isfile(os.path.join(diretorio, arquivo)) and fnmatch.fnmatch(arquivo, '*.zip'):
            contador_zip += 1

    print(f"Arquivos em Fila {contador_zip}")
