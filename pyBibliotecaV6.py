import glob
import json
import os
import re
import psycopg2
import requests
import time
import shutil
import zipfile
from markdownify import markdownify as md
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Configs
load_dotenv()

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")
DIRLOG = os.getenv("DIRLOG")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DebugMode = False
PrintSql = True

# Função para remover duplicatas de msgLogs
def remove_duplicates_msg_logs(msg_logs):
    seen = set()
    unique_logs = []

    for log in msg_logs:
        log_tuple = (
            log.get('Timestamp'),
            log.get('MessageId'),
            log.get('Sender', None),  # Usa None como valor padrão se 'Sender' não existir
            log.get('Recipients', None),  # Usa None como valor padrão se 'Recipients' não existir
            log.get('SenderIp', None)  # Usa None como valor padrão se 'SenderIp' não existir
        )
        if log_tuple not in seen:
            seen.add(log_tuple)
            unique_logs.append(log)

    return unique_logs


# Função para remover duplicatas de callLogs
def remove_duplicates_call_logs(call_logs):
    seen = set()
    unique_calls = []

    for call in call_logs:
        call_tuple = (call['CallId'], call['CallCreator'])
        if call_tuple not in seen:
            seen.add(call_tuple)

            # Verifica e remove duplicatas dentro de Events
            if 'Events' in call:
                call['Events'] = remove_duplicates_events(call['Events'])

            unique_calls.append(call)

    return unique_calls


# Função para remover duplicatas de Events
def remove_duplicates_events(events):
    seen = set()
    unique_events = []

    for event in events:
        event_tuple = (
        event['Timestamp'], event['Type'], event['From'], event['To'], event['FromIp'], event['FromPort'])
        if event_tuple not in seen:
            seen.add(event_tuple)
            unique_events.append(event)

    return unique_events

def limpar_arquivos_antigos(diretorio, dias=5):
    # Obter o timestamp atual
    agora = time.time()
    limite = agora - dias * 86400  # 86400 segundos = 1 dia

    # Verificar todos os arquivos no diretório
    for arquivo in os.listdir(diretorio):
        caminho_arquivo = os.path.join(diretorio, arquivo)

        # Verifica se é um arquivo regular
        if os.path.isfile(caminho_arquivo):
            # Obter a data de modificação do arquivo
            ultima_modificacao = os.path.getmtime(caminho_arquivo)

            # Apaga arquivos antigos
            if ultima_modificacao < limite:
                print(f"\nApagando {arquivo}...")
                os.remove(caminho_arquivo)

    # print("\nLimpeza concluída.")


def replace_divs(html):
    html = str

    return html


def html_to_markdown(html):
    # Substituir a <div class="p"> por um espaço em branco
    pattern = r'<div class="[a-z]"></div>'
    html = re.sub(pattern, ' ', html)

    # Usar BeautifulSoup para manipular o HTML
    soup = BeautifulSoup(html, 'html.parser')

    # Encontrar todas as divs com a classe 'pageBreak' e removê-las
    for div in soup.find_all('div', class_='pageBreak'):
        div.decompose()  # Remove a tag e seu conteúdo

    # Converter o HTML modificado para Markdown
    markdown = md(str(soup), strip=['div'])

    return markdown


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

def openJson(file):
    with open(file, 'r', encoding='utf-8') as arquivo:
        dados = json.load(arquivo)
    return dados


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


def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False

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

    # countdown(5)
    # print('\n')

    if not os.path.isdir(destinationFolder):
        shutil.copytree(OutPutDie, destinationFolder)
        # countdown(5)
        # print('\n')

    return OutPutDie


def removeFolderFiles(FolderPath):
    if os.path.exists(FolderPath):
        shutil.rmtree(FolderPath)


def ListaAllHtml(folderZip):
    FileHtmls = []
    # Percorrer a pasta
    for raiz, diretorios, arquivos in os.walk(folderZip):
        for arquivo in arquivos:
            if arquivo.endswith('.html'):
                FileHtmls.append(f"{raiz}/{arquivo}")

    return FileHtmls

def parsetHTLMFileString(FileHtml):
    markdown_content = None

    print_color(FileHtml, 32)

    if os.path.exists(FileHtml):
        with open(FileHtml, 'r', encoding='utf-8') as file:
            content = file.read()
            markdown_content = html_to_markdown(content)

    return markdown_content

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

def SaveParameters(fileProcess, dataType):
    indice = 0

    Service = fileProcess.get('Service', None)
    InternalTicketNumber = fileProcess.get('InternalTicketNumber', None)
    AccountIdentifier = fileProcess.get('AccountIdentifier', None)
    AccountType = fileProcess.get('AccountType', None)
    Generated = fileProcess.get('UserGenerated', None)
    DateRange = fileProcess.get('DateRange', None)
    FileName = fileProcess.get('FileName', None)
    Unidade = fileProcess.get('Unidade', None)
    NomeUnidade = fileProcess.get('NomeUnidade', None)

    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.unid_id = {Unidade} AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"

        queryLinId = None
        indice += 1
        try:
            db.execute(sqllinh_id)

            if PrintSql:
                print_color(f"3S {indice} - {sqllinh_id}", 32)

            queryLinId = db.fetchone()
        except Exception as e:
            print_color(f"3E {indice} - {sqllinh_id} {e}", 31)
            pass

        if queryLinId is not None and queryLinId[0] > 0:

            linh_id = queryLinId[0]

            sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}'"
            indice += 1
            try:
                db.execute(sqlexistente)
                if PrintSql:
                    print_color(f"4S {indice} - {sqlexistente}", 32)

                queryExiste = db.fetchone()
            except Exception as e:
                print_color(f"4E {indice} - {sqlexistente} {e}", 31)
                pass

            if queryExiste is None:
                ar_id = None

                if 'DADOS' == dataType:
                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1) RETURNING ar_id"

                if 'PRTT' == dataType:
                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 0, 1) RETURNING ar_id"

                if "GDADOS" == dataType:
                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 2, 1) RETURNING ar_id"

                indice += 1
                try:
                    db.execute(sqlInsert)

                    if PrintSql:
                        print_color(f"5S {indice} - {sqlInsert}", 32)

                    con.commit()
                    result = db.fetchone()
                    if result is not None and result[0] is not None:
                        ar_id = result[0]
                except Exception as e:
                    print_color(f"5E {indice} - {sqlInsert} {e}", 31)
                    db.execute("rollback")
                    pass

            else:
                print(f"{queryExiste[0]}")
        else:
            print_color(f"\nLinha Não Localizada {AccountIdentifier}", 31)

    db.close()
    con.close()

    return ar_id