# -*- coding: utf-8 -*-
# !/usr/bin/python3
import json
import os
import time
import re
import shutil
import gc

from dotenv import load_dotenv
from datetime import datetime
from pyBibliotecaV6 import checkFolder, StatusServidor, printTimeData, unzipBase, print_color, \
    parsetHTLMFileString, grava_log, getUnidadeFileName, removeFolderFiles, delete_log,  \
    remover_espacos_regex, somentenumero,  limpar_arquivos_antigos, remove_duplicates_msg_logs, remove_duplicates_call_logs, ListaAllHtml, remove_duplicate_newlines, openJsonEstruturado, contar_arquivos_zip
from pyPostgresql import find_unidade_postgres, listaProcessamento, saveResponse
from pySendElement import sendMessageElement, getroomIdElement
from pyGravandoDados import sendDataPostgres
from pyGetSendApi import sendDataJsonServer

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")
DIRLOG = os.getenv("DIRLOG")

ACCESSTOKEN = os.getenv("ACCESSTOKEN")

DebugMode = False
Executar = True
FileJsonLog = True
TypeProcess = 1 # 1 - Python 2 - PHP

def get_files_in_dir(path):
    return set(os.listdir(path))


def process(source):
    limpar_arquivos_antigos(DIRLOG, dias=5)

    gc.collect()

    fileProcess = {}
    fileDados = {}

    source, Unidade = getUnidadeFileName(source)

    fileName = source.replace(DIRNOVOS, "")
    folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)

    FileHtmls = ListaAllHtml(folderZip)

    flagGDados = None
    flagPrtt = None
    flagDados = None
    dataType = None

    bsHtml = ""

    listaProcessamento(fileName, Unidade)

    for FileHtml in FileHtmls:
        bsHtml += FileHtml
        bsHtml += parsetHTLMFileString(FileHtml)

    bsHtml = remove_duplicate_newlines(bsHtml.replace('![]', ''))

    if bsHtml is not None and bsHtml != "" and Unidade is not None:

        # delete_log(f'{DIRLOG}{os.path.splitext(fileName)[0]}.txt')

        # grava_log(bsHtml, f'{os.path.splitext(fileName)[0]}.txt')

        NomeUnidade = find_unidade_postgres(Unidade)

        print_color(f'\nDESCOMPACTADO {fileName} DA UNIDADE {NomeUnidade} CODIGO {Unidade} \n', 34)

        parsed_json_parameters = parse_dynamic_sentence_parameters(bsHtml)

        if parsed_json_parameters is not None:
            AccountIdentifier = somentenumero(parsed_json_parameters['AccountIdentifier'])
            parsed_json_parameters['AccountIdentifier'] = AccountIdentifier
            fileProcess = parsed_json_parameters

            fileProcess['FileName'] = fileName
            fileProcess['Unidade'] = Unidade
            fileProcess['NomeUnidade'] = NomeUnidade

            if len(AccountIdentifier) > 16:
                flagGDados = True
                dataType = "GDADOS"

            if 'Message Log' in bsHtml or 'Call Logs' in bsHtml:
                flagPrtt = True
                dataType = "PRTT"

            if 'Ncmec Reports' in bsHtml or 'Emails' in bsHtml or 'Connection Info' in bsHtml or 'Web Info' in bsHtml or 'Groups Info' in bsHtml or 'Address Book Info' in bsHtml or 'Small Medium Business' in bsHtml or 'Device Info' in bsHtml:
                flagDados = True
                dataType = "DADOS"

            if flagGDados:
                print_color(f'QUEBRA DE {AccountIdentifier} {dataType}', 92)

                parsed_json_group = parse_dynamic_sentence_group_participants(bsHtml)

                if parsed_json_group is not None:
                    fileDados['groupsInfo'] = parsed_json_group

                dataType = "GDADOS"
                fileProcess['dataType'] = dataType
                fileProcess["GDados"] = fileDados

                if DebugMode:
                    print_color(f"{json.dumps(fileProcess, indent=4)}", 34)
            else:
                print_color(f'QUEBRA DE CONTA {AccountIdentifier} {dataType}', 92)

                if flagPrtt:
                    parsed_json_messages = parse_dynamic_sentence_messages(bsHtml)
                    if parsed_json_messages is not None:
                        fileDados['msgLogs'] = parsed_json_messages

                    parsed_json_calls = parse_dynamic_sentence_calls(bsHtml)
                    if parsed_json_calls is not None:
                        fileDados['callLogs'] = parsed_json_calls

                    dataType = "PRTT"
                    fileProcess['dataType'] = dataType
                    fileProcess["Prtt"] = fileDados

                    if "webInfo" in fileProcess["Prtt"]:
                        del fileProcess["Prtt"]["webInfo"]

                    if "groupsInfo" in fileProcess["Prtt"]:
                        del fileProcess["Prtt"]["groupsInfo"]

                    # Verificação e processamento de callLogs
                    if 'callLogs' in fileProcess['Prtt']:
                        fileProcess['Prtt']['callLogs'] = remove_duplicates_call_logs(
                            fileProcess['Prtt']['callLogs'])

                    # Verificação e processamento de msgLogs
                    if 'msgLogs' in fileProcess['Prtt']:
                        fileProcess['Prtt']['msgLogs'] = remove_duplicates_msg_logs(fileProcess['Prtt']['msgLogs'])

                if flagDados:
                    parsed_json_books = parse_dynamic_sentence_books(bsHtml)
                    if parsed_json_books is not None:
                        fileDados['addressBookInfo'] = parsed_json_books

                    parsed_json_ip_addresses = parse_dynamic_sentence_ip_addresses(bsHtml)
                    if parsed_json_ip_addresses is not None:
                        fileDados['ipAddresses'] = parsed_json_ip_addresses

                    parsed_json_connection = parse_dynamic_sentence_connection(bsHtml)
                    if parsed_json_connection is not None:
                        fileDados['connectionInfo'] = parsed_json_connection

                    parsed_json_device = parse_dynamic_sentence_device(bsHtml)
                    if parsed_json_device is not None:
                        fileDados['deviceinfo'] = parsed_json_device

                    parsed_json_group = parse_dynamic_sentence_group(bsHtml)
                    if parsed_json_group is not None:
                        fileDados['groupsInfo'] = parsed_json_group

                    parsed_json_web = parse_dynamic_sentence_web(bsHtml)
                    if parsed_json_web is not None:
                        fileDados['webInfo'] = parsed_json_web

                    parsed_json_small = parse_dynamic_sentence_small(bsHtml)
                    if parsed_json_small is not None:
                        fileDados['smallmediumbusinessinfo'] = parsed_json_small

                    dataType = "DADOS"
                    fileProcess['dataType'] = dataType
                    fileProcess["Dados"] = fileDados

                if DebugMode:
                    print_color(f"{json.dumps(fileProcess, indent=4)}", 34)

            if Executar:
                if FileJsonLog:
                    readerJsonFile = f'Log_{dataType}_Out_{os.path.splitext(fileName)[0]}.json'

                    json_formatado = json.dumps(fileProcess, indent=2, ensure_ascii=False)

                    delete_log(f"{DIRLOG}{readerJsonFile}")

                    grava_log(json_formatado, readerJsonFile)

                if flagGDados:
                    print_color(
                        f"\n=========================== PROCESSANDO QUEBRA DE GRUPO {fileName} Unidade {Unidade} {NomeUnidade} {dataType}===========================",
                        33)

                if flagDados or flagPrtt:
                    print_color(
                        f"\n=========================== PROCESSANDO QUEBRA DE CONTA {fileName} Unidade {Unidade} {NomeUnidade} {dataType} ===========================",
                        33)

                if TypeProcess == 1:
                    # Processando com Python
                    returno = sendDataPostgres(fileProcess, dataType)
                    exibirRetonoPython(returno, Unidade, fileName, AccountIdentifier, folderZip, source, NomeUnidade)
                else:
                    # Processando com PHP
                    retornoJson = sendDataJsonServer(fileProcess, dataType)
                    exibirRetornoPHP(retornoJson, fileProcess , fileName, Unidade, NomeUnidade, folderZip, source, AccountIdentifier)

            else:
                print_color(
                    f"\n================= PROCESSAMENTO DESLIGADO {fileName} Unidade {Unidade} {NomeUnidade} {dataType}=================",
                    31)
        else:

            print_color(f"Erro Arquivo: {fileName} Unidade: {Unidade}", 31)

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
    else:
        print_color(f"Erro Arquivo: {fileName} Unidade: {Unidade}", 31)

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
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Padrão regex abrangente para capturar todos os campos de cada bloco de informações
    pattern = re.compile(r"""
            Picture\s+\((.+?)\)\s+                   # Captura o caminho da imagem 'Picture'
            Linked\s+Media\s+File:\s*(.+?)\s+        # Captura o caminho do arquivo 'Linked Media File'
            Thumbnail\s+\((.+?)\)\s+                 # Captura o caminho da imagem 'Thumbnail'
            Linked\s+Media\s+File:\s*(.+?)\s+        # Captura o caminho do arquivo 'Linked Media File' da thumbnail
            ID\s*(\d+)\s+                            # Captura o ID numérico
            Creation\s*(\d{4}\-\d{2}\-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)\s+  # Captura a data e hora de criação
            Size\s*(\d+)\s+                          # Captura o tamanho
            Subject\s*(.+?)(?=\s+Picture|$)          # Captura o assunto até a próxima ocorrência de 'Picture' ou fim do texto
        """, re.VERBOSE)

    # Estruturas para armazenar os dados dos grupos
    owned_groups = []
    participating_groups = []

    # Dicionário de campos para mapear os dados
    group_keys = [
        remover_espacos_regex(key) for key in [
            "Picture", "Linked Media File", "Thumbnail", "Thumbnail Linked Media File",
            "ID", "Creation", "Size", "Subject"
        ]
    ]

    # Divide o conteúdo em seções de grupos
    group_sections = re.split(
        r"(GroupsOwned|Owned Groups|GroupsParticipating|Participating Groups)",
        sentence
    )

    current_section = None

    for section in group_sections:
        section = section.strip()

        # Identifica se está nos grupos 'Owned' ou 'Participating'
        if re.match(r"(GroupsOwned|Owned Groups)", section):
            current_section = 'owned'
        elif re.match(r"(GroupsParticipating|Participating Groups)", section):
            current_section = 'participating'
        elif current_section:  # Processa os blocos de dados dentro da seção atual
            for match in pattern.finditer(section):
                group_data = {key: match.group(i + 1).strip() for i, key in enumerate(group_keys)}

                if current_section == 'owned':
                    owned_groups.append(group_data)
                elif current_section == 'participating':
                    participating_groups.append(group_data)

    # Estrutura final dos resultados
    results = {
        "ownedGroups": owned_groups,
        "participatingGroups": participating_groups
    }

    return results if owned_groups or participating_groups else None


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
        results[remover_espacos_regex(key)].extend(cleaned_numbers)

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
    # Remove barras invertidas e espaços desnecessários
    sentence = re.sub(r'\\', '', content.strip())
    sentence = re.sub(r'^\s*[\r\n]', '', sentence, flags=re.M)  # Remove linhas vazias

    # Padrões para capturar campos de chamadas e eventos
    call_pattern = r'Call Id\s*([\w\d]+)\s*Call Creator\s*([\d]+)\s*(Events.*?)(?=Call Id|$)'
    event_pattern = r'Type\s*(\w+)\s*Timestamp\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\s*(?:From\s*([\d]*)\s*To\s*([\d]*)\s*)?(?:From Ip\s*([\d\.:a-fA-F]+)\s*From Port\s*(\d+))?(?:\s*Media Type\s*([\w]+))?'
    participant_pattern = r'Phone Number\s*([\d]+)\s*State\s*(\w+)\s*Platform\s*(\w+)'

    # Encontrar todas as chamadas
    calls = re.findall(call_pattern, sentence, re.S)

    results = []

    for call in calls:
        call_id = call[0].strip()
        call_creator = call[1].strip()
        events_section = call[2]

        # Dicionário para armazenar os resultados
        result = {
            "CallId": call_id,
            "CallCreator": call_creator,
            "Events": []
        }

        # Encontrar todos os eventos dentro da seção de eventos
        events = re.findall(event_pattern, events_section, re.S)
        for event in events:
            event_data = {
                "Type": event[0].strip(),
                "Timestamp": event[1].strip(),
                "From": event[2].strip() if event[2] else '',
                "To": event[3].strip() if event[3] else '',
                "FromIp": event[4].strip() if event[4] else '',
                "FromPort": event[5].strip() if event[5] else '',
                "Participants": []
            }

            # Adicionar Media Type se disponível
            if len(event) > 6 and event[6]:
                event_data["MediaType"] = event[6].strip()

            # Verificar se há participantes
            participants = re.findall(participant_pattern, events_section, re.S)

            for participant in participants:
                participant_data = {
                    "PhoneNumber": participant[0].strip(),
                    "State": participant[1].strip(),
                    "Platform": participant[2].strip()
                }
                event_data["Participants"].append(participant_data)

            result["Events"].append(event_data)

        results.append(result)

    return results if results else None


def exibirRetornoPHP(retornoJson, fileProcess , fileName, Unidade, NomeUnidade, folderZip, source, AccountIdentifier):

    EventoGravaBanco = False

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

    if EventoGravaBanco:
        removeFolderFiles(folderZip)

        saveResponse(AccountIdentifier, Unidade)

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


def exibirRetonoPython(returno, Unidade, fileName, AccountIdentifier, folderZip, source, NomeUnidade):

    if not returno['BANCO']:
        roomId = getroomIdElement(Unidade)

        if roomId is not None:
            sendMessageElement(ACCESSTOKEN, roomId, fileName)

        print_color(
            f"\nERRO GRAVAÇÃO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
            31)
    else:
        removeFolderFiles(folderZip)

        saveResponse(AccountIdentifier, Unidade)

        filePath = DIRLIDOS + fileName

        if not os.path.exists(filePath):
            shutil.move(source, DIRLIDOS)
        else:
            delete_log(source)
            print_color(
                f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                32)


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
