import glob
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
DIRLOG = os.getenv("DIRLOG")

DebugMode = False


def getUnidadeFileName(nome_original):
    FileName, Unidade = None, None

    if "_" in nome_original:
        DadosUnidade = nome_original.split("_")

        Unidade = DadosUnidade[1].replace(".zip", "")

        FileName = f"{DadosUnidade[0]}.zip";

        os.rename(nome_original, FileName)
    else:
        Unidade = 1

        FileName = nome_original

    return FileName, Unidade


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
    arquivo = f"{DIRLOG}{arquivo}"
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
    else: # ARQUIVO PADRÃO ANTIGO
        htmlFile = folderZip + "/index.html"
        if os.path.exists(htmlFile):
            with open(htmlFile, 'r', encoding='utf-8') as f:
                contents = f.read()
                soupHtml = BeautifulSoup(contents, 'html.parser')
                soupHtml.prettify()

    return soupHtml


def contar_arquivos_zip(diretorio):
    # Constrói o padrão de busca para arquivos ZIP
    padrao_busca = os.path.join(diretorio, '*.zip')

    # Usa glob.glob para encontrar todos os arquivos que correspondem ao padrão
    arquivos_zip = glob.glob(padrao_busca)

    # Retorna o número de arquivos ZIP encontrados
    print(f"\nArquivos em Fila {len(arquivos_zip)} {arquivos_zip}\n")


def get_size(path):
    size = os.path.getsize(path)
    return size
    # if size < 1024:
    #     return size  # f"{size} bytes"
    # elif size < 1024 * 1024:
    #     return round(size / 1024, 2)  # f"{round(size / 1024, 2)} KB"
    # elif size < 1024 * 1024 * 1024:
    #     return round(size / (1024 * 1024), 2)  # f"{round(size / (1024 * 1024), 2)} MB"
    # elif size < 1024 * 1024 * 1024 * 1024:
    #     return round(size / (1024 * 1024 * 1024), 2)  # f"{round(size / (1024 * 1024 * 1024), 2)} GB"


def listar_arquivos_json(path):
    # Utiliza o módulo glob para encontrar todos os arquivos com a extensão .json na pasta especificada
    padrao = os.path.join(path, '*.json')
    arquivos_json = glob.glob(padrao)

    return arquivos_json


def openErrorJson():
    path = f"{os.getcwd()}/log"

    arquivos_json = listar_arquivos_json(path)

    for dados_json in arquivos_json:
        if 'Log_Error' in dados_json:
            listNameFile = dados_json.split("_")
            dataType = listNameFile[2]
            fileName = listNameFile[3]

            openJsonEstruturado(dados_json)


def tipoHtml(parameters):
    if 'display:table-cell' in str(parameters):
        print_color(f"\nPENULTIMO PADRÃO HTML display:table-cell", 36)
        tag1 = "div_table"
        tag2 = "font-weight: bold; display:table;"
        tag3 = "font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;"
        tag4 = "display:table-cell"

        print(f"\nTag = {tag1}, {tag2}, {tag3}, {tag4}")

        return tag1, tag2, tag3, tag4

    elif 'div_table outer' in str(parameters):
        print_color(f"\nULTIMO PADRÃO HTML div_table outer", 36)

        tag1 = "div_table outer"
        tag2 = "div_table inner"
        tag3 = "most_inner"
        tag4 = "most_inner"

        print(f"\nTag = {tag1}, {tag2}, {tag3}, {tag4}")

        return tag1, tag2, tag3, tag4

    elif 't o' in str(parameters):
        print_color(f"\nULTIMO PADRÃO HTML t o", 36)

        tag1 = "t o"
        tag2 = "t i"
        tag3 = "m"
        tag4 = "m"

        print(f"\nTag = {tag1}, {tag2}, {tag3}, {tag4}")

        return tag1, tag2, tag3, tag4

    else:
        print_color(f"\nPADRÃO NÃO IDENTIFICADO", 31)
        return None, None, None, None
