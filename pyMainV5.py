# -*- coding: utf-8 -*-
# !/usr/bin/python3
import json
import os
import time
import re
import shutil

from dotenv import load_dotenv
from datetime import datetime
from pyBibliotecaV5 import checkFolder, StatusServidor, printTimeData, unzipBase, print_color, \
    parsetHTLMFileString, grava_log, getUnidadeFileName, removeFolderFiles, delete_log, get_size, contar_arquivos_zip, \
    openJsonEstruturado, remover_espacos_regex, somentenumero, is_valid_json
from pyGravandoDados import sendDataPostgres
from pyPostgresql import find_unidade_postgres
from pyGetSendApi import sendDataJsonServer
from pySendElement import sendMessageElement, getroomIdElement

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")
DIRLOG = os.getenv("DIRLOG")

ACCESSTOKEN = os.getenv("ACCESSTOKEN")

DebugMode = False
Out = False
Executar = True
FileJsonLog = False
ReaderTxt = False


def get_files_in_dir(path):
    return set(os.listdir(path))


def process(source):
    # countdown(1)

    fileProcess = {}
    fileDados = {}

    source, Unidade = getUnidadeFileName(source)

    fileName = source.replace(DIRNOVOS, "")
    folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
    bsHtml = parsetHTLMFileString(folderZip)

    dataType = None

    try:
        if bsHtml is not None and bsHtml != "" and Unidade is not None:

            NomeUnidade = find_unidade_postgres(Unidade)

            print_color(f'\nDESCOMPACTADO {fileName} DA UNIDADE {NomeUnidade} CODIGO {Unidade} \n', 34)

            flagDados = False
            flagPrtt = False
            flagGDados = False

            parsed_json_parameters = parse_dynamic_sentence_parameters(bsHtml)
            if parsed_json_parameters is not None:
                AccountIdentifier = somentenumero(parsed_json_parameters['AccountIdentifier'])
                parsed_json_parameters['AccountIdentifier'] = AccountIdentifier
                fileProcess = parsed_json_parameters

                if Out:
                    print("\nRequest Parameters")
                    print(f"{json.dumps(parsed_json_parameters, indent=4)}")

            if len(AccountIdentifier) > 16:
                print_color(f'QUEBRA DE GRUPO {AccountIdentifier}', 92)

                fileProcess['FileName'] = fileName
                fileProcess['Unidade'] = Unidade
                fileProcess['NomeUnidade'] = NomeUnidade

                parsed_json_group = parse_dynamic_sentence_group_participants(bsHtml)

                if parsed_json_group is not None:
                    flagGDados = True
                    flagDados = False
                    flagPrtt = False

                    fileDados['groupsInfo'] = parsed_json_group

                    if Out:
                        print("\nGroup")
                        print(f"{json.dumps(parsed_json_group, indent=4)}")

                if flagGDados:
                    dataType = "GDADOS"
                    fileProcess["GDados"] = fileDados

                if DebugMode:
                    print_color(f"{json.dumps(fileProcess, indent=4)}", 34)

                EventoGravaBanco = None

                if Executar:
                    if FileJsonLog:
                        readerJsonFile = f'Log_Except_{dataType}_Out_{os.path.splitext(fileName)[0]}.json'

                        json_formatado = json.dumps(fileProcess, indent=2, ensure_ascii=False)
                        grava_log(json_formatado, readerJsonFile)

                    sizeFile = get_size(source)

                    print_color(f"\nTAMANHO DO PACOTE {sizeFile}", 32)

                    print_color(
                        f"\n=========================== ENVIADO PHP QUEBRA DE GRUPO {fileName} Unidade {Unidade} {NomeUnidade} ===========================",
                        33)

                    retornoJson = sendDataJsonServer(fileProcess, dataType)

                    if 'MostraJsonPython' in retornoJson['jsonRetorno']:

                        Jsondata = json.loads(retornoJson['jsonRetorno'])

                        if Jsondata['MostraJsonPython']:
                            print_color(f"\nJSON PROCESSADO", 92)
                            print_color(f"{fileProcess}", 92)

                        if Jsondata['RetornoPHP']:
                            print_color(f"\nRETORNO DO PHP", 34)
                            openJsonEstruturado(Jsondata)

                        if Jsondata['ExibirTotalPacotesFila']:
                            contar_arquivos_zip(DIRNOVOS)

                        if Jsondata['GravaBanco']:
                            print_color(
                                f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                                32)
                            EventoGravaBanco = True
                        else:
                            print_color(
                                f"\nERRO GRAVAÇÃO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                                31)
                else:
                    print_color(
                        f"\n================= ENVIO PHP/PYTHON DESLIGADO {fileName} Unidade {Unidade} {NomeUnidade} =================",
                        31)

                if EventoGravaBanco:
                    removeFolderFiles(folderZip)

                    filePath = DIRLIDOS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRLIDOS)
                    else:
                        delete_log(source)
                else:
                    filePath = DIRERROS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRERROS)

                        roomId = getroomIdElement(Unidade)

                        if roomId is not None:
                            sendMessageElement(ACCESSTOKEN, roomId, fileName)

                        # Novo nome do arquivo
                        new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

                        # Renomeia o arquivo
                        os.rename(filePath, new_filename)
                    else:
                        os.remove(source)

                    removeFolderFiles(folderZip)

                print_color(
                    f"\n================================= Fim {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} =================================",
                    35)

            else:
                print_color(f'QUEBRA DE CONTA {AccountIdentifier}', 92)

                fileProcess['FileName'] = fileName
                fileProcess['Unidade'] = Unidade
                fileProcess['NomeUnidade'] = NomeUnidade

                parsed_json_books = parse_dynamic_sentence_books(bsHtml)
                if parsed_json_books is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['addressBookInfo'] = parsed_json_books

                    if Out:
                        print("\nAddress Book Info")
                        print(f"{json.dumps(parsed_json_books, indent=4)}")

                parsed_json_ip_addresses = parse_dynamic_sentence_ip_addresses(bsHtml)
                if parsed_json_ip_addresses is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['ipAddresses'] = parsed_json_ip_addresses

                    if Out:
                        print("\nIp Addresses")
                        print(f"{json.dumps(parsed_json_ip_addresses, indent=4)}")

                parsed_json_connection = parse_dynamic_sentence_connection(bsHtml)
                if parsed_json_connection is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['connectionInfo'] = parsed_json_connection

                    if Out:
                        print("\nConnection")
                        print(f"{json.dumps(parsed_json_connection, indent=4)}")

                parsed_json_device = parse_dynamic_sentence_device(bsHtml)
                if parsed_json_device is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['deviceinfo'] = parsed_json_device

                    if Out:
                        print("\nDevice")
                        print(f"{json.dumps(parsed_json_device, indent=4)}")

                parsed_json_group = parse_dynamic_sentence_group(bsHtml)
                if parsed_json_group is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['groupsInfo'] = parsed_json_group

                    if Out:
                        print("\nGroup")
                        print(f"{json.dumps(parsed_json_group, indent=4)}")

                parsed_json_web = parse_dynamic_sentence_web(bsHtml)
                if parsed_json_web is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['webInfo'] = parsed_json_web

                    if Out:
                        print("\nWeb")
                        print(f"{json.dumps(parsed_json_web, indent=4)}")

                parsed_json_small = parse_dynamic_sentence_small(bsHtml)
                if parsed_json_small is not None:
                    flagDados = True
                    flagPrtt = False
                    flagGDados = False

                    fileDados['smallmediumbusinessinfo'] = parsed_json_small

                    if Out:
                        print("\nSmall")
                        print(f"{json.dumps(parsed_json_small, indent=4)}")

                parsed_json_messages = parse_dynamic_sentence_messages(bsHtml)
                if parsed_json_messages is not None:
                    flagDados = False
                    flagPrtt = True
                    flagGDados = False

                    fileDados['msgLogs'] = parsed_json_messages

                    if Out:
                        print("\nMessages")
                        print(f"{json.dumps(parsed_json_messages, indent=4)}")

                parsed_json_calls = parse_dynamic_sentence_calls(bsHtml)
                if parsed_json_calls is not None:
                    flagDados = False
                    flagPrtt = True
                    flagGDados = False

                    fileDados['callLogs'] = parsed_json_calls

                    if Out:
                        print("\nCalls")
                        print(f"{json.dumps(parsed_json_calls, indent=4)}")

                if flagDados:
                    dataType = "DADOS"
                    fileProcess["Dados"] = fileDados

                if flagPrtt:
                    dataType = "PRTT"
                    fileProcess["Prtt"] = fileDados

                    if "webInfo" in fileProcess["Prtt"]:
                        del fileProcess["Prtt"]["webInfo"]

                    if "groupsInfo" in fileProcess["Prtt"]:
                        del fileProcess["Prtt"]["groupsInfo"]

                if flagGDados:
                    dataType = "GDADOS"
                    fileProcess["GDados"] = fileDados

                if DebugMode:
                    print_color(f"{json.dumps(fileProcess, indent=4)}", 34)

                EventoGravaBanco = None

                if Executar:
                    if FileJsonLog:
                        readerJsonFile = f'Log_Except_{dataType}_Out_{os.path.splitext(fileName)[0]}.json'

                        json_formatado = json.dumps(fileProcess, indent=2, ensure_ascii=False)
                        grava_log(json_formatado, readerJsonFile)

                    sizeFile = get_size(source)

                    print_color(f"\nTAMANHO DO PACOTE {sizeFile}", 32)

                    print_color(
                        f"\n=========================== ENVIADO PHP QUEBRA DE CONTA {fileName} Unidade {Unidade} {NomeUnidade} ===========================",
                        33)

                    retornoJson = sendDataJsonServer(fileProcess, dataType)

                    if 'MostraJsonPython' in retornoJson['jsonRetorno']:

                        Jsondata = json.loads(retornoJson['jsonRetorno'])

                        if Jsondata['MostraJsonPython']:
                            print_color(f"\nJSON PROCESSADO", 92)
                            print_color(f"{fileProcess}", 92)

                        if Jsondata['RetornoPHP']:
                            print_color(f"\nRETORNO DO PHP", 34)
                            openJsonEstruturado(Jsondata)

                        if Jsondata['ExibirTotalPacotesFila']:
                            contar_arquivos_zip(DIRNOVOS)

                        if Jsondata['GravaBanco']:
                            print_color(
                                f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                                32)
                            EventoGravaBanco = True
                        else:
                            print_color(
                                f"\nERRO GRAVAÇÃO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                                31)
                else:
                    print_color(
                        f"\n================= ENVIO PHP/PYTHON DESLIGADO {fileName} Unidade {Unidade} {NomeUnidade} =================",
                        31)

                if EventoGravaBanco:
                    removeFolderFiles(folderZip)

                    filePath = DIRLIDOS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRLIDOS)
                    else:
                        delete_log(source)
                else:
                    filePath = DIRERROS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRERROS)

                        roomId = getroomIdElement(Unidade)

                        if roomId is not None and Executar:
                            sendMessageElement(ACCESSTOKEN, roomId, fileName)

                        # Novo nome do arquivo
                        new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

                        # Renomeia o arquivo
                        os.rename(filePath, new_filename)
                    else:
                        os.remove(source)

                    removeFolderFiles(folderZip)

                print_color(
                    f"\n================================= Fim {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} =================================",
                    35)

        else:
            print_color(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 31)

            grava_log(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 'LogPadraoAntigo.txt')

    except Exception as inst:

        if is_valid_json(fileProcess):
            sendDataPostgres(fileProcess, dataType, Out)

        print_color(f"Location: process - Files Open, error: {str(inst)} File: {str(source)}", 31)

        if FileJsonLog:
            readerJsonFile = f'Log_Except_{dataType}_Out_{os.path.splitext(fileName)[0]}.json'

            json_formatado = json.dumps(fileProcess, indent=2, ensure_ascii=False)
            grava_log(json_formatado, readerJsonFile)


        filePath = DIRERROS + fileName

        if not os.path.exists(filePath):
            shutil.move(source, DIRERROS)

            roomId = getroomIdElement(Unidade)

            if roomId is not None:
                sendMessageElement(ACCESSTOKEN, roomId, fileName)

            # Novo nome do arquivo
            new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

            # Renomeia o arquivo
            os.rename(filePath, new_filename)
        else:
            os.remove(source)

        removeFolderFiles(folderZip)

    if DebugMode:
        print("\nMovendo de: ", source)
        print("Para: ", DIRLIDOS)
        print("Arquivo Finalizado!\n")
        print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")
    else:
        print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")


def parse_dynamic_sentence_parameters(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    patterns = {
        "Service": r"Service(\w+)",
        "Internal Ticket Number": r"Internal Ticket Number(\d+)",
        "Account Identifier": r"Account Identifier\s*([\+\d\s-]+)",
        "Account Type": r"Account Type(\w+)",
        "User Generated": r"Generated(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Date Range": r"Date Range(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC to \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Ncmec Reports Definition": r"Ncmec Reports Definition(NCMEC Reports: [\w\s\(\)]+)",
        "NCMEC CyberTip Numbers": r"NCMEC CyberTip Numbers([\w\s]+)",
        "Emails Definition": r"Emails Definition(Emails: [\w\s':]+)",
        "Registered Email Addresses": r"Registered Email Addresses([\w\s]+)"
    }

    if ReaderTxt:
        grava_log(sentence, 'arquivo.txt')
        exit()

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            results[remover_espacos_regex(key)] = match.group(1).strip()

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_books(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    results = []

    # Dicionário para armazenar os resultados
    data = {}

    # Expressão regular para capturar números de telefone
    phone_number_pattern = r"\b\d{11,}\b"

    # Dividir a frase em seções "Symmetric" e "Asymmetric"
    symmetric_section = re.search(r"Symmetric contacts\d+ Total ([\d\s]+)", sentence)
    asymmetric_section = re.search(r"Asymmetric contacts\d+ Total ([\d\s]+)", sentence)

    symmetric_numbers = []
    asymmetric_numbers = []

    # Extrair números de telefone da seção "Symmetric"
    if symmetric_section:
        symmetric_numbers = re.findall(phone_number_pattern, symmetric_section.group(1))

    # Extrair números de telefone da seção "Asymmetric"
    if asymmetric_section:
        asymmetric_numbers = re.findall(phone_number_pattern, asymmetric_section.group(1))

    data['Symmetriccontacts'] = symmetric_numbers
    data['Asymmetriccontacts'] = asymmetric_numbers

    results.append(data)

    if len(data['Symmetriccontacts']) > 0 or len(data['Asymmetriccontacts']) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_ip_addresses(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressão regular para capturar os pares de "Time" e "IP Address"
    time_ip_pattern = re.compile(r"Time(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\s+IP Address([0-9a-fA-F\.:]+)")

    # Capturar todos os pares de "Time" e "IP Address" da frase
    time_ip_matches = time_ip_pattern.findall(sentence)

    # Criar uma lista de dicionários para as conexões
    results = [{"Time": time, "IPAddress": ip} for time, ip in time_ip_matches]

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_connection(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da conexão
    patterns = {
        "Device Id": r"Device Id(\d+)",
        "Service start": r"Service start(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Device Type": r"Device Type(\w+)",
        "App Version": r"App Version([\d\.]+)",
        "Device OS Build Numberos": r"Device OS Build Number([\w\s:]+)",
        "Connection State": r"Connection State(\w+)",
        "Online Since": r"Online Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Connected from": r"Connected from([\w\.:]+)",
        "Push Name": r"Push Name(.*)"
    }

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            results[remover_espacos_regex(key)] = match.group(1).strip() if key != "Push Name" else match.group(
                1).strip()

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_device(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da informação do dispositivo
    patterns = {
        "Device Id": r"Device Id(\d+)",
        "App Version": r"App Version([\w\-\.]+)",
        "OS Version": r"OS Version([\w\.]+)",
        "OS Build Number": r"OS Build Number\s*([\w\s]*)",
        "Device Manufacturer": r"Device Manufacturer([\w\s]+)",
        "Device Model": r"Device Model([\w\s]+)"
    }

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            results[remover_espacos_regex(key)] = match.group(1).strip()

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_group(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos dos grupos
    group_patterns = {
        "Linked Media File": r"Linked Media File:([\w\\/_\.-]+)",
        "Thumbnail": r"Thumbnail([\w\s]+)",
        "ID": r"ID(\d+)",
        "Creation": r"Creation(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Size": r"Size(\d+)",
        "Subject": r"Subject([\w\s]+)"
    }

    # Estruturas para armazenar as informações dos grupos
    owned_groups = []
    participating_groups = []

    # Capturar informações dos grupos Owned
    owned_section = re.search(r"GroupsOwned.*$", sentence, re.DOTALL)

    if owned_section is None:
        owned_section = re.search(r"Owned Groups.*$", sentence, re.DOTALL)

    if owned_section:
        owned_section_text = owned_section.group(0)
        owned_group = {}
        for key, pattern in group_patterns.items():
            match = re.search(pattern, owned_section_text)
            if match:
                owned_group[remover_espacos_regex(key)] = match.group(1).strip()
        owned_groups.append(owned_group)

    # Capturar informações dos grupos Participating
    participating_section = re.search(r"GroupsParticipating.*$", sentence, re.DOTALL)

    if participating_section is None:
        participating_section = re.search(r"Participating Groups.*$", sentence, re.DOTALL)

    if participating_section:
        participating_section_text = participating_section.group(0)
        participating_group = {}
        for key, pattern in group_patterns.items():
            match = re.search(pattern, participating_section_text)
            if match:
                participating_group[remover_espacos_regex(key)] = match.group(1).strip()
        participating_groups.append(participating_group)

    # Dicionário para armazenar os resultados
    results = {
        "ownedGroups": owned_groups,
        "ParticipatingGroups": participating_groups
    }

    if len(owned_groups) > 0 or len(participating_groups) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_group_participants(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressão regular para capturar números de telefone de qualquer grupo
    pattern = r"(GroupParticipants|GroupAdministrators|Participants)\s*\d+\s+Total\s+([\d\s]+)"

    # Dicionário para armazenar os resultados
    results = {
        "GroupParticipants": [],
        "GroupAdministrators": [],
        "Participants": []
    }

    # Captura de dados usando regex
    matches = re.findall(pattern, sentence, re.IGNORECASE)

    # Itera sobre cada correspondência encontrada
    for key, numbers in matches:
        # Limpa e separa os números
        cleaned_numbers = re.findall(r'\d+', numbers)
        results[key].extend(cleaned_numbers)

    # Verifica se há resultados e retorna, caso contrário, retorna None
    return results if any(results.values()) else None


def parse_dynamic_sentence_web(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da informação do dispositivo
    patterns = {
        "Version": r"Version([\w\.]+)",
        "Platform": r"Platform([\w\.]+)",
        "Device Manufacturer": r"Device Manufacturer([\w\s]+)",
        "Online Since": r"Online Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Inactive Since": r"Inactive Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)"
    }

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            results[remover_espacos_regex(key)] = match.group(1).strip()

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_small(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da informação do dispositivo
    patterns = {
        "Small Medium Business": r"Small Medium Business([\w\.]+)",
        "Address": r"Address([\w\.]+)",
        "Email": r"Email([\w\.]+)",
        "Name": r"Name([\w\.]+)",
    }

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            results[remover_espacos_regex(key)] = match.group(1).strip()

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_messages(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da mensagem
    message_patterns = {
        "Timestamp": r"Timestamp(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Message Id": r"Message Id([\w\d]+)",
        "Sender": r"Sender(\d+)",
        "Recipients": r"Recipients(\d+)",
        "Sender Ip": r"Sender Ip([\d\.:a-fA-F]+)",
        "Sender Port": r"Sender Port(\d+)",
        "Sender Device": r"Sender Device([\w]+)",
        "Type": r"Type([\w]+)",
        "Message Style": r"Message Style([\w]+)",
        "Message Size": r"Message Size(\d+)"
    }

    # Dividir a string em blocos de mensagens individuais usando a presença de 'Timestamp' e 'Message Size' como delimitadores
    messages = re.findall(r'(Timestamp\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC.*?Message Size\d+)', sentence, re.DOTALL)

    results = []

    for message in messages:
        # Dicionário para armazenar os resultados
        result = {}
        # Iterar sobre os padrões e encontrar as correspondências
        for key, pattern in message_patterns.items():
            match = re.search(pattern, message)
            if match:
                result[remover_espacos_regex(key)] = match.group(1).strip()
        # Adicionar apenas se result não estiver vazio
        if result:
            results.append(result)

    if len(results) > 0:
        return results
    else:
        return None


def parse_dynamic_sentence_calls(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressões regulares para capturar os campos da chamada, eventos e participantes
    call_pattern = r'Call Id\s*([\w\d]+)\s*Call Creator\s*([\d]+)\s*(Events.*?)(?=Call Id|$)'
    event_pattern = r'Type\s*([\w]+)\s*Timestamp\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\s*From\s*([\d]+)\s*To\s*([\d]*)\s*From Ip\s*([\d\.:a-fA-F]+)\s*From Port\s*(\d+)(?:\s*Media Type\s*([\w]+))?'
    participant_pattern = r'Phone Number\s*([\d]+)\s*State\s*([\w]+)\s*Platform\s*([\w]+)'

    # Encontrar todas as chamadas
    calls = re.findall(call_pattern, sentence, re.DOTALL)

    results = []

    for call in calls:
        call_id, call_creator, events_section = call

        # Dicionário para armazenar os resultados
        result = {
            "CallId": call_id.strip(),
            "CallCreator": call_creator.strip(),
            "Events": []
        }

        # Encontrar todos os eventos dentro da seção de eventos
        events = re.findall(event_pattern, events_section, re.DOTALL)
        for event in events:
            event_data = {
                "Type": event[0].strip(),
                "Timestamp": event[1].strip(),
                "From": event[2].strip(),
                "To": event[3].strip(),
                "FromIp": event[4].strip(),
                "FromPort": event[5].strip(),
                "Participants": []
            }

            # Adicionar Media Type se disponível
            if len(event) > 6 and event[6]:
                event_data["MediaType"] = event[6].strip()

            # Encontrar todos os participantes dentro do evento
            participant_section = sentence[sentence.find('Participants', sentence.find(event[1])):]
            participants = re.findall(participant_pattern, participant_section, re.DOTALL)

            for participant in participants:
                participant_data = {
                    "PhoneNumber": participant[0],
                    "State": participant[1],
                    "Platform": participant[2]
                }
                event_data["Participants"].append(participant_data)

            result["Events"].append(event_data)

        results.append(result)

    return results if results else None


if __name__ == '__main__':
    checkFolder(DIRNOVOS)
    checkFolder(DIRLIDOS)
    checkFolder(DIRERROS)
    checkFolder(DIREXTRACAO)
    checkFolder(DIRLOG)

    previous_files = get_files_in_dir(DIRNOVOS)

    dttmpstatus = ""
    print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

    while True:
        time.sleep(3)
        current_files = get_files_in_dir(DIRNOVOS)
        added_files = current_files - previous_files
        removed_files = previous_files - current_files

        try:
            if added_files:
                for file in added_files:
                    process(f"{DIRNOVOS}{file}")
            if removed_files:
                print(f'\nArquivos removidos: {removed_files}')
        except Exception as inst:
            pass

        previous_files = current_files

        result = StatusServidor(dttmpstatus)
        if result == True:
            dttmpstatus = datetime.today().strftime('%Y%m%d%H%M%S')
        else:
            printTimeData()
