import os, re
from dotenv import load_dotenv

# Configs
load_dotenv()

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

DebugMode = False


def getUnidadeFileName(nome_original):
    FileName, Unidade = None, None

    if "_" in nome_original:
        DadosUnidade = nome_original.split("_")

        Unidade = DadosUnidade[1].replace(".txt", "")

        FileName = f"{DadosUnidade[0]}.txt";

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