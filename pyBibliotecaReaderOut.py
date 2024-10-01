import os, re
import shutil
import zipfile
from markdownify import markdownify as md
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Configs
load_dotenv()

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

DebugMode = False

def delete_log(nome_arquivo):
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)
        print_color(f"\nExcluido {nome_arquivo}", 31)

def removeFolderFiles(FolderPath):
    if os.path.exists(FolderPath):
        shutil.rmtree(FolderPath)

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

def getUnidadeFileName(nome_original):

    if "_" in nome_original:
        DadosUnidade = nome_original.split("_")

        Unidade = DadosUnidade[1].replace(".zip", "")

        FileName = f"{DadosUnidade[0]}.zip"

        os.rename(nome_original, FileName)
    else:
        Unidade = 1

        FileName = nome_original

    return FileName, Unidade


def remover_espacos_regex(texto):
    return re.sub(r"\s", "", texto)


def somentenumero(parametro):
    return re.sub('[^0-9]', '', parametro)


def grava_log(content, arquivo):
    arquivo = f"{os.getcwd()}/log/{arquivo}"
    with open(arquivo, "a") as text_file:
        text_file.write('{}\n'.format(content) + '\n')
    text_file.close()


def print_color(text, color_code):
    print(f"\033[{color_code}m{text}\033[0m")


def checkFolder(FolderPath):
    if not os.path.exists(FolderPath):
        os.makedirs(FolderPath)