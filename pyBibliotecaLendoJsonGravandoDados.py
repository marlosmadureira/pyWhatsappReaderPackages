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

DebugMode = False

def get_files_in_dir(path):
    return set(os.listdir(path))

def limpar_arquivos_antigos(diretorio, dias=10):
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

    print("\nLimpeza concluída.")


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


def localizar_erro_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as arquivo:
        for i, linha in enumerate(arquivo, start=1):
            try:
                json.loads(linha)
            except json.JSONDecodeError as e:
                print(f"Erro na linha {i}: {e}")
                return i

def corrigir_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as arquivo:
            conteudo = arquivo.read()

        # Corrigir strings não terminadas
        conteudo_corrigido = re.sub(r'(?<!\\)"(?![,\s:}])', r'\"', conteudo)

        # Salvar o arquivo corrigido para revisão
        with open('arquivo_corrigido.json', 'w', encoding='utf-8') as arquivo_corrigido:
            arquivo_corrigido.write(conteudo_corrigido)

        # Tentar carregar o JSON corrigido
        dados = json.loads(conteudo_corrigido)
        print("JSON carregado e corrigido com sucesso!")
        return dados

    except json.JSONDecodeError as e:
        print(f"Erro ao tentar carregar o JSON corrigido: {e}")
        return None

    except Exception as e:
        print(f"Erro inesperado: {e}")
        return None


# Função para remover duplicatas de msgLogs
def remove_duplicates_msg_logs(msg_logs):
    seen = set()
    unique_logs = []

    for log in msg_logs:
        log_tuple = (log['Timestamp'], log.get('MessageId'), log['Sender'], log['Recipients'], log['SenderIp'])
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

def openJson(file):
    try:
        with open(file, 'r', encoding='utf-8') as arquivo:
            dados = json.load(arquivo)
            if 'msgLogs' in dados['Prtt']:
                dados['Prtt']['msgLogs'] = remove_duplicates_msg_logs(dados['Prtt']['msgLogs'])

            # Verificação e processamento de callLogs
            if 'callLogs' in dados['Prtt']:
                dados['Prtt']['callLogs'] = remove_duplicates_call_logs(dados['Prtt']['callLogs'])
        return dados
    except FileNotFoundError:
        print_color(f"Erro: O arquivo '{file}' não foi encontrado.", 31)
    except json.JSONDecodeError as e:
        print_color(f"Erro: Arquivo '{file}' não é um JSON válido. Detalhes: {e}",31)
    except Exception as e:
        print_color(f"Erro inesperado ao tentar abrir o arquivo '{file}': {e}",31)
    return None


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


def parsetHTLMFileString(folderZip):
    htmlFile = folderZip + "/records.html"

    markdown_content = None

    if os.path.exists(htmlFile):
        with open(htmlFile, 'r', encoding='utf-8') as file:
            content = file.read()
            markdown_content = html_to_markdown(content)
    else: # ARQUIVO PADRÃO ANTIGO
        htmlFile = folderZip + "/index.html"
        if os.path.exists(htmlFile):
            with open(htmlFile, 'r', encoding='utf-8') as file:
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
