# -*- coding: utf-8 -*-
# !/usr/bin/python3
import json
import os
import time
import re
import shutil
import gc
import traceback
import psycopg2
import html

from dotenv import load_dotenv
from datetime import datetime
from pyBibliotecaV71 import checkFolder, StatusServidor, printTimeData, unzipBase, print_color, \
    parsetHTLMFileString, grava_log, getUnidadeFileName, removeFolderFiles, delete_log,  \
    remover_espacos_regex, somentenumero,  limpar_arquivos_antigos, remove_duplicates_msg_logs, remove_duplicates_call_logs, ListaAllHtml, remove_duplicate_newlines, openJsonEstruturado, contar_arquivos_zip
from pyPostgresql import find_unidade_postgres, listaProcessamento, saveResponse
from pySendElement import sendMessageElement, getroomIdElement
from pyGravandoDados import sendDataPostgres
from pyGetSendApi import sendDataJsonServer

# Configs
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")
DIRLOG = os.getenv("DIRLOG")

ACCESSTOKEN = os.getenv("ACCESSTOKEN")

DebugMode = False
Executar = True
FileJsonLog = True
TypeProcess = 2 # 1 - Python 2 - PHP

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)

def get_files_in_dir(path):
    return set(os.listdir(path))

def finalizar_arquivo(source, folderZip, destino, fileName, Unidade, roomIds, motivo=None):
    try:
        if motivo:
            print_color(motivo, 31)

            if roomIds:
                msg = f"🚨 ERRO PROCESSAMENTO\nArquivo: {fileName}\nMotivo: {motivo}"
                for roomId in roomIds:
                    sendMessageElement(ACCESSTOKEN, roomId[0], msg)

        if folderZip:
            removeFolderFiles(folderZip)

        if destino == 'ERRO':
            target = DIRERROS + fileName
        else:
            target = DIRLIDOS + fileName

        if not os.path.exists(target) and os.path.exists(source):
            shutil.move(source, os.path.dirname(target))
            # print_color('MOVIDO1', 32)
        elif os.path.exists(source):
            os.remove(source)

    except Exception as e:
        print_color(f"[ERRO FINALIZAÇÃO] {e}", 31)

def process(source):
    limpar_arquivos_antigos(DIRLOG, dias=3)
    source = os.path.abspath(source)

    gc.collect()

    fileProcess = {}
    fileDados = {}

    # Apenas extrai informações — NÃO renomeia nada
    fileName, Unidade = getUnidadeFileName(source)

    # Garante caminho absoluto e consistente
    source = os.path.abspath(source)

    roomIds = getroomIdElement(Unidade)

    fileName = source.replace(DIRNOVOS, "")
    folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
    if folderZip is None:
        finalizar_arquivo(
            source, None, 'ERRO', fileName, Unidade, roomIds,
            motivo="ZIP inválido ou não foi possível extrair"
        )
        return

    FileHtmls, msgElementNewFile = ListaAllHtml(folderZip)

    if msgElementNewFile != "" and roomIds is not None:
        msgElement = f"NOVO ARQUIVO {fileName} WHATSAPP IDENTIFICADO {msgElementNewFile}"

        print_color(f"\nEnvio da Mensagem {msgElement}", 33)

        for roomId in roomIds:
            elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
            print_color(f"{elementLog}", 33)
            time.sleep(2)

    flagGDados = None
    flagPrtt = None
    flagDados = None
    dataType = None

    bsHtml = ""

    listaProcessamento(fileName, Unidade)

    records_html_raw = ""
    records_html_converted = ""

    preservation_html_raw = ""
    preservation_html_converted = ""

    other_html_raw = ""
    other_html_converted = ""

    # Separar HTMLs por tipo
    for FileHtml in FileHtmls:
        if "records" in FileHtml.lower():  # arquivo correto
            records_html_raw += FileHtml
            records_html_converted += parsetHTLMFileString(FileHtml)
        elif "preservation" in FileHtml.lower():  # preservations
            preservation_html_raw += FileHtml
            preservation_html_converted += parsetHTLMFileString(FileHtml)
        else:  # outros htmls
            other_html_raw += FileHtml
            other_html_converted += parsetHTLMFileString(FileHtml)

    # Primeiro: processar SOMENTE o records.html para extrair parâmetro seguro
    bsHtml_records = (records_html_raw + records_html_converted).strip()

    parsed_json_parameters = parse_dynamic_sentence_parameters(bsHtml_records)

    if parsed_json_parameters is None:
        print_color("ERRO: Não foi possível extrair parâmetros do records.html", 31)
        return

    # Extrair somente o número do records
    AccountIdentifier = somentenumero(parsed_json_parameters["AccountIdentifier"])
    parsed_json_parameters["AccountIdentifier"] = AccountIdentifier

    # Agora sim, juntar todos os HTMLs para processar mensagens/dados
    bsHtml = (
            records_html_raw + records_html_converted +
            preservation_html_raw + preservation_html_converted +
            other_html_raw + other_html_converted
    )

    bsHtml = remove_duplicate_newlines(bsHtml.replace('![]', ''))

    if bsHtml is not None and bsHtml != "" and Unidade is not None:
        NomeUnidade = find_unidade_postgres(Unidade)

        print_color(f'\nDESCOMPACTADO {fileName} DA UNIDADE {NomeUnidade} CODIGO {Unidade} \n', 34)

        parsed_json_parameters = parse_dynamic_sentence_parameters(bsHtml)

        if parsed_json_parameters is not None:
            AccountIdentifier = somentenumero(parsed_json_parameters['AccountIdentifier'])
            parsed_json_parameters['AccountIdentifier'] = AccountIdentifier
            # print("bsHTML -------> ", bsHtml)
            fileProcess.update(parsed_json_parameters)
            fileProcess['FileName'] = fileName
            fileProcess['Unidade'] = Unidade
            fileProcess['NomeUnidade'] = NomeUnidade
            # fileProcess['rawProcessedMarkdown'] = bsHtml

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
                print_color(f'QUEBRA DA CONTA {AccountIdentifier} {dataType}', 92)
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

                    parsed_json_group = parse_dynamic_sentence_groupNew(bsHtml)

                    #parsed_json_group = parse_dynamic_sentence_group(bsHtml)
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
                        f"\n=========================== INICIO PROCESSANDO QUEBRA DE GRUPO {fileName} Unidade {Unidade} {NomeUnidade} {dataType}===========================",
                        35)

                if flagDados or flagPrtt:
                    print_color(
                        f"\n=========================== INICIO PROCESSANDO QUEBRA DE CONTA {fileName} Unidade {Unidade} {NomeUnidade} {dataType} ===========================",
                        35)

                if TypeProcess == 1:
                    # Processando com Python
                    returno = sendDataPostgres(fileProcess, dataType)
                    exibirRetonoPython(returno, Unidade, fileName, AccountIdentifier, folderZip, source, NomeUnidade, flagDados, roomIds)
                else:
                    # Processando com PHP
                    # print(fileProcess)
                    retornoJson = sendDataJsonServer(fileProcess, dataType)
                    exibirRetornoPHP(retornoJson, fileProcess , fileName, Unidade, NomeUnidade, folderZip, source, AccountIdentifier, flagDados, roomIds)
            else:
                print_color(
                    f"\n================= PROCESSAMENTO DESLIGADO {fileName} Unidade {Unidade} {NomeUnidade} {dataType}=================",
                    31)
        else:

            print_color(f"Erro Arquivo: {fileName} Unidade: {Unidade}", 31)

            filePath = DIRERROS + fileName

            if roomIds is not None:
                msgElement = f"ERRO DE PROCESSAMENTO ARQUIVO WHATSAPP {fileName}"

                print_color(f"\nEnvio da Mensagem {msgElement}", 33)

                for roomId in roomIds:
                    elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                    print_color(f"{elementLog}", 33)
                    time.sleep(2)

            if not os.path.exists(filePath):
                shutil.move(source, DIRERROS)
                print_color('MOVIDO2', 32)

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

        if roomIds is not None:
            msgElement = f"ERRO DE PROCESSAMENTO ARQUIVO WHATSAPP {fileName}"

            print_color(f"\nEnvio da Mensagem {msgElement}", 33)

            for roomId in roomIds:
                elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                print_color(f"{elementLog}", 33)
                time.sleep(2)

        if not os.path.exists(filePath):
            shutil.move(source, DIRERROS)
            print_color('MOVIDO3', 32)

            # Novo nome do arquivo
            new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

            # Renomeia o arquivo
            os.rename(filePath, new_filename)
        else:
            os.remove(source)

        removeFolderFiles(folderZip)

    print_color(
        f"\n================================= FIM PROCESSAMENTO {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} =================================",
        35)

    if DebugMode:
        print("\nMovendo de: ", source)
        print("Para: ", DIRLIDOS)
        print("Arquivo Finalizado!\n")
        print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads - Banco: {DB_HOST}\n")
    else:
        print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads - Banco: {DB_HOST}\n")

def parse_dynamic_sentence_parameters(content):
    # --- Vetor de ignorados expandido ---
    # print("Content------------>",content)
    ignored_patterns = [
        r"WhatsApp Business Record Page\s*\d+",
        r"\[.*?\]\(.*?\)",  # Links markdown [texto](url)
        r"Links to Records",
        r"Preservation-\d+",
        r"Records\s*Date Range:",
        r"Date Generated:",
        r"Categories\s*\*\s*\[Request Parameters\]\(#property-request_parameters\)\s*\*\s*\[Message Log\]\(#property-message_log\)\s*\*\s*\[Call Logs\]\(#property-call_logs\)", # Adicionado para remover a seção de categorias
        r"Print Options\s*\*\s*By Category All Request Parameters Message Log Call Logs\s*\*\s*By Page: Page Range Page Number", # Adicionado para remover a seção de opções de impressão
        r"Print\s*", # Adicionado para remover a palavra "Print" que precede "Service"
    ]

    # Remove barras invertidas, espaços extras e padrões ignorados
    sentence = re.sub(r'\\', '', content).strip()
    for pattern in ignored_patterns:
        sentence = re.sub(pattern, "", sentence, flags=re.DOTALL)

    # Remove linhas vazias
    sentence = ' '.join(line for line in sentence.splitlines() if line.strip()) # Alterado para juntar com espaço, não com '\n'
    sentence = re.sub(r'\s+', ' ', sentence).strip() # Normaliza múltiplos espaços para um único espaço

    # --- Lista de campos para lookahead ---
    fields = [
        "Service", "Internal Ticket Number", "Account Identifier",
        "Account Type", "Generated", "Date Range",
        "Ncmec Reports Definition", "NCMEC CyberTip Numbers",
        "Emails Definition", "Registered Email Addresses",
        "Connection Info Definition", "Message Log", "Ip Addresses Definition",
        "Ip Addresses", "Profile Picture Definition", "Profile Picture"
    ]

    # Lookahead para parar no próximo campo
    # Adicionei \s* antes do $ para permitir espaços no final antes do fim da string
    lookahead = "|".join(re.escape(f) for f in fields) + r"|\s*$"

    # --- Regex individuais com lookahead robusto ---
    regex_fields = {
        "Service": re.compile(
            r"Service\s*:?\s*(.*?)(?=" + lookahead + r")", # Alterado de \w+? para .*?
            re.DOTALL
        ),
        "InternalTicketNumber": re.compile(
            r"Internal Ticket Number\s*:?\s*(.*?)(?=" + lookahead + r")", # Alterado de \d+? para .*?
            re.DOTALL
        ),
        "AccountIdentifier": re.compile(
            r"Account Identifier\s*:?\s*([\+\d\s\-,]+?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "AccountType": re.compile(
            r"Account Type\s*:?\s*(.*?)(?=" + lookahead + r")", # Alterado de \w+? para .*?
            re.DOTALL
        ),
        "Generated": re.compile(
            r"Generated\s*:?\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)",
            re.DOTALL
        ),
        "DateRange": re.compile(
            r"Date Range\s*:?\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC\s+to\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)",
            re.DOTALL
        ),
        "NcmecReportsDefinition": re.compile(
            r"Ncmec Reports Definition\s*:?\s*(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "NCMECCyberTips": re.compile(
            r"NCMEC CyberTip(?:s| Numbers)\s*:?\s*(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "EmailsDefinition": re.compile(
            r"Emails Definition\s*:?\s*(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "RegisteredEmailAddresses": re.compile(
            r"Registered Email Addresses\s*:?\s*(.*?)(?=Ip Addresses Definition|" + lookahead + r")",
            re.DOTALL
        )
    }

    # Função para limpar valores capturados
    def clean_value(value, field_name=None):
        if not value:
            return None

        # Remove links markdown [texto](url)
        value = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', value)

        # Remove tags HTML (se ainda houver alguma, embora html_to_markdown deva ter cuidado com isso)
        value = re.sub(r'<[^>]+>', '', value)

        if field_name == "RegisteredEmailAddresses":
            # Esta lógica pode precisar de ajuste se o lookahead já estiver funcionando bem
            value = re.split(r'(?=Ip Addresses Definition)', value)[0]
        # Remove parênteses e colchetes extras no final
        value = re.sub(r'[\]\)]+\s*$', '', value)
        value = re.sub(r'^\s*[\[\(]+', '', value)

        # Remove ":" no início
        value = re.sub(r'^\s*:\s*', '', value)

        # Remove texto após marcadores específicos (para Generated e DateRange)
        if field_name in ['Generated', 'DateRange']:
            value = re.sub(r'\).*$', '', value)
            value = re.sub(r'\(.*$', '', value)

        # Limpa espaços extras e quebras de linha
        value = ' '.join(value.split())
        value = value.strip()

        # Verifica se é vazio ou "No responsive records"
        if not value or value.lower() in ['', 'no responsive records']:
            return "No responsive records"

        if field_name == "AccountIdentifier":
            # tira vírgulas extras no fim
            value = value.rstrip(",")

        return value

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in regex_fields.items():
        match = pattern.search(sentence)
        if match:
            value = match.group(1)
            value = clean_value(value, key)
            results[key] = value
        else:
            results[key] = None

    if len(results) > 0:
        return results
    else:
        return None

def parse_dynamic_sentence_books(content):
    """
    Versão FINAL - Funciona com texto GRUDADO!
    """
    import re

    # PASSO 1: LIMPEZA
    sentence = content
    sentence = re.sub(r'\\', '', sentence)
    sentence = re.sub(r'WhatsApp\s+Business\s+Record\s+Page\s*\d+', '', sentence, flags=re.IGNORECASE)
    sentence = re.sub(r'WhatsApp\s+Business\s+Record', '', sentence, flags=re.IGNORECASE)
    sentence = re.sub(r'Page\s+\d+\s+of\s+\d+', '', sentence, flags=re.IGNORECASE)
    sentence = re.sub(r'\s+', ' ', sentence)
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    results = []
    data = {}

    # Expressão regular para capturar números de telefone
    phone_number_pattern = r"\b\d{11,}\b"

    # ✅ REGEX CORRETA: SEM espaço obrigatório entre "contacts" e número
    symmetric_section = re.search(
        r"Symmetric\s+contacts\s*(\d+)\s*Total\s*(.*?)(?=Asymmetric|$)",
        # r"Symmetric\s+contacts(\d+)\s+Total\s+(.*?)(?=Asymmetric|$)",
        sentence,
        re.IGNORECASE | re.DOTALL
    )

    # ✅ REGEX CORRETA: SEM espaço obrigatório entre "contacts" e número
    # Captura até encontrar texto não-numérico significativo
    asymmetric_section = re.search(
        r"Asymmetric\s+contacts\s*(\d+)\s*Total\s*([\d\s]+?)(?=[A-Za-z]{5,}|$)",
        # r"Asymmetric\s+contacts(\d+)\s+Total([\d\s]+?)(?=[A-Za-z]{5,}|$)",
        sentence,
        re.IGNORECASE
    )

    symmetric_numbers = []
    asymmetric_numbers = []

    # Função robusta para dividir números muito longos
    def split_long_numbers(numbers, label=""):
        """
        Divide números muito longos de forma inteligente.
        """
        valid_lengths = [15, 14, 13, 12, 11]

        result = []
        total_original = len(numbers)
        total_split = 0

        print(f"\n📊 Processando {label}:")
        print(f"   Números originais: {total_original}")

        for num in numbers:
            num_len = len(num)

            # Se o número tem tamanho válido, mantém
            if num_len in valid_lengths:
                result.append(num)
                continue

            # Se for muito longo, tenta dividir
            if num_len > 15:
                divided = False

                # Tenta divisões em partes IGUAIS
                for length in valid_lengths:
                    if num_len % length == 0:
                        chunks = [num[i:i + length] for i in range(0, num_len, length)]
                        print(f"   ✂️ Dividido (igual): {num} → {chunks}")
                        result.extend(chunks)
                        total_split += len(chunks) - 1
                        divided = True
                        break

                # Se não conseguiu divisão igual, tenta divisões MISTAS
                if not divided:
                    for length in valid_lengths:
                        if num_len > length:
                            remaining = num_len - length
                            if remaining in valid_lengths:
                                chunk1 = num[:length]
                                chunk2 = num[length:]
                                print(f"   ✂️ Dividido (misto): {num} → [{chunk1}, {chunk2}]")
                                result.extend([chunk1, chunk2])
                                total_split += 1
                                divided = True
                                break

                # Se não conseguiu dividir, mantém como está
                if not divided:
                    print(f"   ⚠️ Não dividido: {num} ({num_len} dígitos)")
                    result.append(num)
            else:
                result.append(num)

        print(f"   ✅ Total após divisão: {len(result)} números")
        if total_split > 0:
            print(f"   📈 {total_split} números foram divididos")

        return result

    # Extrair números de telefone da seção "Symmetric"
    if symmetric_section:
        expected_count = int(symmetric_section.group(1))
        content_text = symmetric_section.group(2)

        symmetric_numbers = re.findall(phone_number_pattern, content_text)
        symmetric_numbers = split_long_numbers(symmetric_numbers, "Symmetric Contacts")

    # Extrair números de telefone da seção "Asymmetric"
    if asymmetric_section:
        expected_count = int(asymmetric_section.group(1))
        content_text = asymmetric_section.group(2)

        asymmetric_numbers = re.findall(phone_number_pattern, content_text)
        asymmetric_numbers = split_long_numbers(asymmetric_numbers, "Asymmetric Contacts")

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
    # --- 1) Limpeza inicial e isolamento do bloco Connection Info ---
    sentence = re.sub(r'\\', '', content).strip()
    sentence = re.sub(r'^.*?Connection\s*Info\s*Definition', '', sentence, flags=re.DOTALL|re.IGNORECASE)
    sentence = re.sub(r'Web\s*Info\s*Definition.*$', '', sentence, flags=re.DOTALL|re.IGNORECASE)
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # --- 2) Definir os campos e montar a lookahead ---
    fields = [
        "Device Id",
        "Service start",
        "Device Type",
        "App Version",
        "Device OS Build Number",
        "Connection State",
        "Online Since",
        "Inactive Since",
        "Last seen",
        "Push Name",
    ]
    # lookahead: para cada padrão, parar antes de qualquer um desses campos ou do fim da string
    lookahead = r"(?=\s*(?:%s)|$)" % "|".join(re.escape(c) for c in fields)

    # --- 3) Regex não-gulosa com lookahead para cada campo ---
    patterns = {
        "DeviceId":            re.compile(r"Device\s*Id\s*(\d+)"    + lookahead, re.IGNORECASE),
        "ServiceStart":        re.compile(r"Service\s*start\s*([^\s].*?)"      + lookahead, re.IGNORECASE),
        "DeviceType":          re.compile(r"Device\s*Type\s*(.*?)"            + lookahead, re.IGNORECASE),
        "AppVersion":          re.compile(r"App\s*Version\s*(.*?)"            + lookahead, re.IGNORECASE),
        "DeviceOSBuildNumber": re.compile(r"Device\s*OS\s*Build\s*Number\s*(.*?)" + lookahead, re.IGNORECASE),
        "ConnectionState":     re.compile(r"Connection\s*State\s*(.*?)"       + lookahead, re.IGNORECASE),
        "OnlineSince":         re.compile(r"Online\s*Since\s*([^\s].*?)"      + lookahead, re.IGNORECASE),
        "InactiveSince":       re.compile(r"Inactive\s*Since\s*([^\s].*?)"      + lookahead, re.IGNORECASE),
        # "LastSeen":            re.compile(r"Last\s*seen\s*([\d\-]{10}\s+[\d:]{8}\s+UTC)" + lookahead, re.IGNORECASE),
        "LastSeen": re.compile(
            r"Last\s*seen\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s+UTC)",
            re.IGNORECASE
        ),
        "PushName":            re.compile(r"Push\s*Name\s*(.*?)"            + lookahead, re.IGNORECASE),
    }

    # --- 4) Quebra em blocos por “Connection Device Id” ---
    blocks = re.split(r"(?=Device\s*Id)", sentence, flags=re.IGNORECASE)
    results = []

    for blk in blocks:
        if not blk.strip():
            continue
        entry = {}
        for k, rx in patterns.items():
            m = rx.search(blk)
            entry[k] = m.group(1).strip() if m else None
        # adiciona só se DeviceId existir
        if entry.get("DeviceId") is not None:
            results.append(entry)

    return results if results else None

def parse_dynamic_sentence_device(content):
    ignored_patterns = [
        r"^.*?(?=Device\s*Info\s*Definition)",
        r"Device Info Definition.*?(?=Device\s*Info\s*Device\s*Id\b)",
    ]

    sentence = re.sub(r'\\', '', content).strip()

    for pattern in ignored_patterns:
        sentence = re.sub(pattern, "", sentence, flags=re.DOTALL | re.IGNORECASE)

    sentence = ' '.join(line for line in sentence.splitlines() if line.strip())
    sentence = re.sub(r'\s+', ' ', sentence).strip()

    # --- Lista de campos para lookahead ---
    fields = [
        "Device Id", "App Version", "OS Version", "OS Build Number",
        "Device Manufacturer", "Device Model"
    ]

    lookahead = r"(?=\s*(?:%s)|$)" % "|".join(re.escape(c) for c in fields)

    regex_fields = {
        "DeviceId": re.compile(
            r"Device Id\s*:?\s*(\d+?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        ),
        "AppVersion": re.compile(
            r"App Version\s*:?\s*(.*?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        ),
        "OSVersion": re.compile(
            r"OS Version\s*:?\s*(.*?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        ),
        "OSBuildNumber": re.compile(
            r"OS Build Number\s*:?\s*(.*?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        ),
        "DeviceManufacturer": re.compile(
            r"Device Manufacturer\s*:?\s*(.*?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        ),
        "DeviceModel": re.compile(
            r"Device Model\s*:?\s*(.*?)(?=" + lookahead + r")", # Adicionado \s*:?\s*
            re.DOTALL
        )
    }

    def extract_devices(text):
        devices = []
        device_blocks = re.split(r'(Device Id\s*:?\s*\d+)', text)
        if device_blocks and not re.match(r'Device Id\s*:?\s*\d+', device_blocks[0]):
            device_blocks = device_blocks[1:]

        processed_blocks = []
        for i in range(0, len(device_blocks), 2):
            if i + 1 < len(device_blocks):
                processed_blocks.append(device_blocks[i] + device_blocks[i+1])
            else:
                processed_blocks.append(device_blocks[i])
        for block in processed_blocks:
            if not block.strip():
                continue
            device_data = {}
            for field, pattern in regex_fields.items():
                match = pattern.search(block)
                if match:
                    value = match.group(1).strip()
                    if field == "DeviceModel" and value:
                        value = value.split('\n')[0].split('/')[0].strip()
                    device_data[field] = value if value else None
                else:
                    device_data[field] = None
            # Só adiciona se tiver pelo menos DeviceId
            if device_data.get("DeviceId") is not None:
                devices.append(device_data)
        seen = set()
        unique_devices = []
        for device in devices:
            device_key = (
                device.get("DeviceId"),
                device.get("AppVersion"),
                device.get("OSVersion"),
                device.get("OSBuildNumber"),
                device.get("DeviceManufacturer"),
                device.get("DeviceModel")
            )
            if device_key not in seen:
                seen.add(device_key)
                unique_devices.append(device)
        # print("unique_devices------------> ", unique_devices)
        return unique_devices

    devices = extract_devices(sentence)

    if len(devices) > 0:
        return devices
    else:
        return None

def parse_dynamic_sentence_groupNew(content):
    # --- Vetor de ignorados ---
    ignored_patterns = [
        r"WhatsApp Business Record Page\s*\d+",
        r"Groups Info Definition.*?(?=Groups\s*(?:Owned Groups|Participating Groups)|Address Book Info|$)"
    ]

    # Remove barras invertidas, espaços extras e padrões ignorados
    sentence = re.sub(r'\\', '', content).strip()
    for pattern in ignored_patterns:
        sentence = re.sub(pattern, "", sentence)

    owned_section = re.search(
        r"Groups Owned Groups\s*(.*?)(?=Participating Groups|Address Book Info|$)",
        sentence,
        re.DOTALL
    )

    participating_section = re.search(
        r"Participating Groups\s*(.*?)(?=Groups Owned Groups|Address Book Info|$)",
        sentence,
        re.DOTALL
    )

    # Regex individuais (cada campo isolado)
    regex_fields = {
        "Picture": re.compile(r"Picture\s*\(?(linked_media/[^)\s]+?\.(?:jpg|jpeg|png))\)?",re.IGNORECASE),
        "LinkedMediaFile": re.compile(r"Linked\s*Media\s*File:\s*(linked_media/[^\s)]+?\.(?:jpg|jpeg|png))",re.IGNORECASE),
        "Thumbnail": re.compile(r"Thumbnail\s*(.*?)(?=ID\s+\d{6,}|Creation|Size|Description|Subject|Picture|Linked Media File|$)",re.DOTALL),
        "ID": re.compile(r"ID\s*([^\s]+)(?=\s*Creation|Size|Subject|Picture|Linked Media File|Thumbnail|Description|$)"),
        "Creation": re.compile(r"Creation\s*([\d-]+\s+[\d:]+\s+UTC)"),
        "Size": re.compile(r"Size\s*(\d+)"),
        "Description": re.compile(r"Description\s*(.*?)(?=Subject|Picture|Linked Media File|Thumbnail|ID\s+\d{6,}|Creation|Size|$)", re.DOTALL),
        "Subject": re.compile(r"Subject\s*(.*?)(?=Picture|Linked Media File|Thumbnail|ID\s+\d{6,}|Creation|Size|Description|$)",re.DOTALL),
    }

    def extract_groups(section_text):
        groups = []
        if section_text:
            group_blocks = re.findall(
                # r'((?:Picture\b|ID\s+\d{6,})\b.*?)(?=(?:Picture\b|ID\s+\d{6,})\b|$)',
                r'((?:Picture\b.*?ID\s+\d{6,}.*?|ID\s+\d{6,}.*?))(?=(?:Picture\b|ID\s+\d{6,})\b|$)',
                section_text,
                re.DOTALL
            )

            for block in group_blocks:
                if not block.strip():
                    continue

                group_data = {}
                for field, pattern in regex_fields.items():
                    match = pattern.search(block)
                    if match:
                        value = match.group(1).strip()
                        # Remove parênteses do campo Picture e LinkedMediaFile
                        if field in ["Picture", "LinkedMediaFile"]:
                            value = value.strip("()")

                        group_data[field] = value
                    else:
                        group_data[field] = None  # default

                # Normalizar valores padrões
                if not group_data["Picture"]:
                    group_data["Picture"] = "No picture"
                if not group_data["LinkedMediaFile"]:
                    group_data["LinkedMediaFile"] = "No linked media"
                if not group_data["Thumbnail"]:
                    group_data["Thumbnail"] = "No thumbnail"
                if not group_data["Description"]:
                    group_data["Description"] = ""

                # Validação obrigatória → só salva se tiver os 4 essenciais
                if (
                        group_data["ID"]
                        and group_data["Creation"]
                        and group_data["Size"]
                        and group_data["Subject"]
                ):
                    groups.append(group_data)

        return groups

    dados = {
        "ownedGroups": extract_groups(owned_section.group(1) if owned_section else ""),
        "ParticipatingGroups": extract_groups(participating_section.group(1) if participating_section else "")
    }
    # print('GRUPOS -----------> ',dados)
    return dados

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
        "ParticipatingGroups": participating_groups
    }

    print_color(f"{results}", 33)

    return results if owned_groups or participating_groups else None

def parse_dynamic_sentence_group_participants(content):
    # Remove as barras invertidas e espaços em branco desnecessários
    sentence = re.sub(r'\\', '', content).strip()
    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # Expressão regular para capturar números de telefone de qualquer grupo
    pattern = r"(Group Participants|Group Administrators|Participants)\s*\d+\s+Total\s+([\d\s]+)"

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
    # --- Vetor de ignorados ---
    ignored_patterns = [
        r"WhatsApp Business Record Page\s*\d+",
        r"Device Info Definition.*?(?=Device Info\s*\d|$)",
        r"Connection Info Definition.*?(?=Connection\s*\d|$)"
    ]

    # Remove barras invertidas, espaços extras e padrões ignorados
    sentence = re.sub(r'\\', '', content).strip()

    for pattern in ignored_patterns:
        sentence = re.sub(pattern, "", sentence, flags=re.DOTALL)

    # Remove linhas vazias
    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # --- Lista de campos para lookahead ---
    fields = [
        "Version", "Platform", "Device Manufacturer",
        "Online Since", "Inactive Since"
    ]

    # Lookahead para parar no próximo campo
    lookahead = "|".join(re.escape(f) for f in fields) + r"|$"

    # --- Regex individuais com lookahead robusto ---
    regex_fields = {
        "Version": re.compile(
            r"Version(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "Platform": re.compile(
            r"Platform(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "DeviceManufacturer": re.compile(
            r"Device Manufacturer(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "OnlineSince": re.compile(
            r"Online Since(.*?)(?=" + lookahead + r")",
            re.DOTALL
        ),
        "InactiveSince": re.compile(
            r"Inactive Since(.*?)(?=" + lookahead + r")",
            re.DOTALL
        )
    }

    # Dicionário para armazenar os resultados
    results = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in regex_fields.items():
        match = pattern.search(sentence)
        if match:
            value = match.group(1).strip()
            results[key] = value if value else None
        else:
            results[key] = None

    if len(results) > 0:
        return results
    else:
        return None

def parse_dynamic_sentence_small(content):
    ignored_patterns = [
        r"Small Medium Business Definition.*?(?=Small Medium Business|Small Medium Business\s|Device Info Definition|$)",
    ]

    sentence = re.sub(r'\\', '', content).strip()
    for pattern in ignored_patterns:
        sentence = re.sub(pattern, "", sentence, flags=re.DOTALL)

    sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

    # campos que queremos capturar
    fields = ["Name", "Email", "Address", "Websites"]

    # tokens adicionais de parada que aparecem no seu documento e que devem interromper a captura
    extra_stop_tokens = ["Device Info Definition","Connection Info","Web Info","Groups Info","Address Book Info","Print Options By Category","All Request Parameters","Ncmec Reports","Device Info",]
    stop_list = fields + extra_stop_tokens
    lookahead = "|".join(re.escape(f) for f in stop_list) + r"|$"

    # Regex mais tolerante: aceita ":" opcional e captura até o lookahead
    regex_fields = {
        "Name": re.compile(r"Small Medium Business Name\s*:?\s*(.*?)(?=" + lookahead + r")", re.DOTALL | re.IGNORECASE),
        "Email": re.compile(r"(?:Email)\s*:?\s*(.*?)(?=" + lookahead + r")", re.DOTALL | re.IGNORECASE),
        "Address": re.compile(r"(?:Address)\s*:?\s*(.*?)(?=" + lookahead + r")", re.DOTALL | re.IGNORECASE),
        "Websites": re.compile(r"(?:Websites)\s*:?\s*(.*?)(?=" + lookahead + r")", re.DOTALL | re.IGNORECASE),
    }

    results = {}

    def extract_email(text):
        # procura um email padrão dentro do texto
        m = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        return m.group(0).strip() if m else None

    def extract_website(text):
        # aceita URLs com http(s) ou www ou domínio com ponto (e sem espaços estranhos)
        m = re.search(r'(https?://[^\s,;]+|www\.[^\s,;]+|[A-Za-z0-9-]+\.[A-Za-z]{2,}(?:/[^\s,;]*)?)', text)
        if not m:
            return None
        cand = m.group(0).strip().rstrip('.')
        # rejeita matches óbvios de frases genéricas como 'Business generated website of business'
        if re.search(r'business generated website', text, re.IGNORECASE):
            return None
        return cand

    # Para cada campo, pega todos os matches e escolhe o último não-vazio (valor real)
    for key, pattern in regex_fields.items():
        matches = pattern.findall(sentence)
        value = None
        if matches:
            for m in reversed(matches):
                if not m:
                    continue
                cand = m.strip()
                cand = re.sub(r"^[:\-\s]+", "", cand).strip()
                if cand:
                    value = cand
                    break

        # validações específicas
        if key == "Email":
            if value:
                email_ok = extract_email(value)
                results[key] = email_ok
            else:
                results[key] = None
        elif key == "Websites":
            if value:
                web_ok = extract_website(value)
                results[key] = web_ok
            else:
                results[key] = None
        else:
            results[key] = value if value else None

    # Se ao menos um campo válido foi encontrado, retorna; caso contrário, None
    if any(v is not None for v in results.values()):
        # opcional: remover valores que são apenas a descrição genérica
        # (por segurança extra)
        for k in ["Email", "Websites"]:
            if results.get(k) is None:
                results[k] = None
        # print("Small --------> ", results)
        return results
    return None

def parse_dynamic_sentence_messages(content):
    try:
        # 1) Limpeza básica
        sentence = re.sub(r'\\', '', content).strip()
        sentence = '\n'.join(line for line in sentence.splitlines() if line.strip())

        # 2) Lista de todos os nomes de campos para usar no lookahead
        fields = [
            "Timestamp", "Message Id", "Sender", "Recipients", "Group Id",
            "Sender Ip", "Sender Port", "Sender Device", "Type",
            "Message Style", "Message Size"
        ]
        # Escapa para uso em regex e monta o lookahead
        lookahead = "|".join(re.escape(f) for f in fields) + r"|$"

        # 3) Definição das regexes não-gulosas
        regex_fields = {
            "Timestamp":    re.compile(r"Timestamp\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "MessageId":    re.compile(r"Message Id\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "Sender":       re.compile(r"Sender\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "Recipients":   re.compile(r"Recipients\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "GroupId":      re.compile(r"Group Id\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "SenderIp":     re.compile(r"Sender Ip\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "SenderPort":   re.compile(r"Sender Port\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "SenderDevice": re.compile(r"Sender Device\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "Type":         re.compile(r"Type\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            "MessageStyle": re.compile(r"Message Style\s*(.*?)(?=" + lookahead + r")", re.DOTALL),
            # "MessageSize":  re.compile(r"Message Size\s*(.*?)(?=Message|Timestamp|$)", re.DOTALL),
            "MessageSize": re.compile(r"Message Size\s*(\d+)"),
        }

        # 4) Quebra em blocos a partir de "Timestamp"
        blocks = re.split(r"(?=Timestamp\s*\d{4}-\d{2}-\d{2}\s)", sentence)

        results = []
        for block in blocks:
            block = block.strip()
            if not block:
                continue

            record = {}
            for key, rx in regex_fields.items():
                m = rx.search(block)
                record[key] = m.group(1).strip() if m else None

            # validação mínima
            if record["Timestamp"] and record["MessageId"] and record["Sender"]:
                results.append(record)

        # 5) Print com a mesma formatação do parse_dynamic_sentence_groupNew
        if results:
            # print_color(f"{results}", 33)
            return results
        else:
            print_color("DEBUG: No messages extracted", 31)
            return None
    except Exception as e:
        print("Erro ao parsear parameters:", e)
        raise

def parse_dynamic_sentence_calls(content):
    # 1) limpa tags HTML
    clean = re.sub(r'<[^>]+>', '', content)

    # 2) isola conteúdo após "Call Logs Definition" (se existir)
    m = re.search(r'Call Logs Definition(.*)', clean, re.DOTALL)
    sentence = m.group(1).strip() if m else clean.strip()

    # 3) remove tudo antes do primeiro "Call Call Id" e barras invertidas
    sentence = re.sub(r'^.*?(?=Call\s*Call Id)', '', sentence, flags=re.DOTALL)
    sentence = sentence.replace('\\', '').strip()

    # 4) regex para CallId e CallCreator
    regex_call = {
        "CallId":      re.compile(r"Call Id\s*([A-Z0-9]{32})(?=\s*Call Creator|\s*Events|$)", re.DOTALL),
        "CallCreator": re.compile(r"Call Creator\s*(\d+)(?=\s*Events|$)", re.DOTALL),
    }

    # 5) prepara lookahead de evento incluindo "Participants"
    event_fields   = ["Type", "Timestamp", "From", "To", "From Ip", "From Port", "Media Type", "Participants"]
    # lookahead_ev   = r"(?=\s*(?:" + "|".join(re.escape(f) for f in event_fields) + r")|$)"
    lookahead_ev = r"(?=\s*(?:%s)|$)" % "|".join(re.escape(c) for c in event_fields)

    # 6) regex de campos de evento
    regex_event = {
        "Type":      re.compile(r"Type\s*(offer|accept|terminate|reject|av_switch|group_update)" + lookahead_ev, re.IGNORECASE),
        "Timestamp": re.compile(r"Timestamp\s*([\d\-:\s]+UTC)" + lookahead_ev),
        "From":      re.compile(r"From\s*(\d+)" + lookahead_ev),
        "To":        re.compile(r"To\s*(\d+)" + lookahead_ev),
        "FromIp":    re.compile(r"From Ip\s*([\d\.:a-f]+)" + lookahead_ev, re.IGNORECASE),
        "FromPort":  re.compile(r"From Port\s*(\d+)" + lookahead_ev),
        "MediaType": re.compile(r"Media Type\s*(audio|video)" + lookahead_ev, re.IGNORECASE),
    }

    results = []
    # 7) separa cada bloco de chamada
    for cblock in re.split(r"(?=Call\s*Call Id)", sentence):
        c = cblock.strip()
        if len(c) < 50:
            continue

        call_rec = {}
        # extrai CallId e CallCreator
        for k, rx in regex_call.items():
            m = rx.search(c)
            call_rec[k] = m.group(1).strip() if m else None

        # se não tiver ID válido, pula
        if not call_rec.get("CallId"):
            continue

        # 8) isola texto de Events (se existir)
        m_ev = re.search(r"Events\s*(.*)", c, re.DOTALL)
        ev_text = m_ev.group(1).strip() if m_ev else c

        # 9) separa blocos de evento por "Type"
        ev_blocks = re.split(r"(?=Type\s*(?:offer|accept|terminate|reject|av_switch|group_update))", ev_text)

        call_rec["Events"] = []
        for eb in ev_blocks:
            eb = eb.strip()
            if len(eb) < 8:
                continue

            ev = {}
            # extrai cada campo
            for k, rx in regex_event.items():
                m = rx.search(eb)
                ev[k] = m.group(1).strip() if m else None

            # --- captura participantes, se existir ---
            ev["Participants"] = []
            idx = eb.find("Participants")
            if idx != -1:
                pr = eb[idx + len("Participants"):].strip()
                part_rx = re.compile(
                    r"Phone Number\s*(\d+)\s*State\s*(\w+)(?:\s*Platform\s*(\w+))?",
                    re.IGNORECASE
                )
                for pm in part_rx.finditer(pr):
                    ev["Participants"].append({
                        "PhoneNumber": pm.group(1),
                        "State":        pm.group(2),
                        "Platform":     pm.group(3) or None
                    })

            # só adiciona evento se tiver Type
            if ev.get("Type"):
                call_rec["Events"].append(ev)

        results.append(call_rec)

    return results or None

def build_element_message(fileName, Unidade, NomeUnidade, AccountIdentifier, type, retorno):
    lines = []

    # HTML PURO - SEM html.escape nas tags!
    lines.append(f"<strong>🚨ALERTA — WhatsApp🚨</strong>")
    lines.append(f"<strong>Arquivo:</strong> {html.escape(fileName)}")
    lines.append(f"<strong>Conta:</strong> {html.escape(AccountIdentifier)}")
    lines.append(f"<strong>Tipo:</strong> {html.escape(str(type))}")

    # Erro de transporte / HTTP / PHP 500
    if isinstance(retorno, dict) and retorno.get('ok') is False and 'error' in retorno:
        err = retorno.get('error', {})
        ctx = retorno.get('context', {})

        lines.append("<strong>Resultado:</strong> <span style='color: red;'>ERROR</span>")
        if ctx.get('request_id'):
            lines.append(f"<strong>request_id:</strong> <code>{html.escape(ctx['request_id'])}</code>")

        # PRIORIDADE 1: response_snippet
        if 'response_snippet' in ctx and ctx['response_snippet']:
            try:
                snippet_json = json.loads(ctx['response_snippet'])
                if isinstance(snippet_json, dict) and 'error' in snippet_json:
                    php_err = snippet_json['error']
                    lines.append(
                        f"<strong>Aviso (PHP):</strong> <span style='color: orange;'>{html.escape(php_err.get('code'))}: {php_err.get('message')}</span>")
                else:
                    lines.append(f"<strong>Aviso (snippet):</strong> {html.escape(ctx['response_snippet'][:400])}...")
            except json.JSONDecodeError:
                lines.append(f"<strong>Aviso (snippet):</strong> {html.escape(ctx['response_snippet'][:400])}...")
        else:
            lines.append(
                f"<strong>Aviso:</strong> <span style='color: red;'>{html.escape(err.get('code'))}: {err.get('message')}</span>")

        return "<br>".join(lines)  # Usa <br> em vez de \n para HTML

    # Resposta PHP normal
    jr = retorno.get('jsonRetorno')
    if isinstance(jr, str):
        try:
            jr = json.loads(jr)
        except Exception:
            jr = {'Resultado': 'ERROR',
                  'Errors': [{'code': 'INVALID_JSONRETORNO', 'message': 'jsonRetorno não parseável'}]}

    Aviso = jr.get('Aviso', []) or []
    errors = jr.get('Errors', []) or []

    if Aviso:
        w0 = Aviso[0]
        lines.append(f"<strong>Aviso:</strong> {html.escape(w0.get('code'))}: {html.escape(w0.get('message'))}")

    if errors:
        e0 = errors[0]
        lines.append(
            f"<strong>Aviso:</strong> <span style='color: orange;'>{html.escape(e0.get('code'))}: {html.escape(e0.get('message'))}</span>")
        ctx = e0.get('context', {}) or {}
        if 'sql' in ctx:
            lines.append("<strong>SQL (trecho):</strong>")
            lines.append(f"<code>{html.escape(str(ctx['sql'])[:900])}</code>")

    return "<br>".join(lines)

def exibirRetornoPHP(retornoJson, fileProcess , fileName, Unidade, NomeUnidade, folderZip, source, AccountIdentifier, flagDados, roomIds):

    EventoGravaBanco = False
    ja_enviou_element = False

    if not isinstance(retornoJson, dict):
        print_color(f"[ERRO] Retorno do PHP em formato inválido: {type(retornoJson)} | valor={str(retornoJson)[:300]}", 31)
        return

    if 'jsonRetorno' not in retornoJson:
        print_color(f"[ERRO] Campo 'jsonRetorno' ausente no retorno: {str(retornoJson)[:800]}", 31)

        # Se quiser alertar no Element aqui (recomendado):
        if roomIds is not None and not ja_enviou_element:
            msgElement = build_element_message(
                fileName=fileName,
                Unidade=Unidade,
                NomeUnidade=NomeUnidade,
                AccountIdentifier=AccountIdentifier,
                type=retornoJson.get('type', 'N/A'),
                retorno=retornoJson
            )
            for roomId in roomIds:
                elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                print_color(f"{elementLog}", 33)
            ja_enviou_element = True

        try:
            if source and os.path.exists(source):
                destino_zip = os.path.join(DIRERROS, fileName)
                shutil.move(source, destino_zip)
                # print_color(f"[ZIP movido para ERROs] {destino_zip}", 31)
            else:
                print_color(f"[AVISO] ZIP não encontrado: {source}", 33)
        except Exception as e:
            print_color(f"[ERRO AO MOVER ZIP PARA ERRO] {e}", 31)
        try:
            if folderZip and os.path.exists(folderZip):
                removeFolderFiles(folderZip)
                # print_color(f"[PASTA REMOVIDA] {folderZip}", 31)
        except Exception as e:
            print_color(f"[ERRO AO REMOVER PASTA] {e}", 31)
        return

    json_retorno = retornoJson.get('jsonRetorno')

    if isinstance(json_retorno, dict):
        Jsondata = json_retorno
    elif isinstance(json_retorno, str):
        try:
            Jsondata = json.loads(json_retorno)
        except json.JSONDecodeError as e:
            print_color(f"[ERRO] jsonRetorno string não parseável: {e} | snippet={json_retorno[:800]}", 31)
            return
    else:
        print_color(f"[ERRO] jsonRetorno em tipo inesperado: {(json_retorno)}", 31)
        return

    if Jsondata.get('MostraJsonPython'):
        print_color("\nJSON PROCESSADO", 92)
        print_color(f"{fileProcess}", 92)

    if Jsondata.get('RetornoPHP'):
        print_color("\nRETORNO DO PHP", 32)
        openJsonEstruturado(Jsondata)

    if Jsondata.get('ExibirTotalPacotesFila'):
        contar_arquivos_zip(DIRNOVOS)

    resultado = Jsondata.get('Resultado')
    if Jsondata.get('GravaBanco') or resultado == 'DUPLICATE':
        print_color(f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}", 32)
        EventoGravaBanco = True
    else:
        print_color(f"\nERRO GRAVAÇÃO NO BANCO DE DADOS(php)!!! {fileName} Unidade {Unidade} {NomeUnidade}", 31)
        EventoGravaBanco = False

    if EventoGravaBanco:
        removeFolderFiles(folderZip)

        destino = os.path.join(DIRLIDOS, fileName)
        # 🔹 NOVO BLOCO — verifica se já existe em LIDOS
        if os.path.exists(destino):
            print_color(" Arquivo apagado, ja existente em lidos!", 33)
            try:
                os.remove(destino)
            except Exception as e:
                print_color(f"[ERRO AO REMOVER ARQUIVO EXISTENTE EM LIDOS] {e}", 31)

        if not os.path.exists(destino):
            if source and os.path.exists(source):
                finalizar_arquivo( source=source, folderZip=folderZip, destino='LIDO', fileName=fileName, Unidade=Unidade, roomIds=roomIds )
                # shutil.move(source, DIRLIDOS)
                # print_color('MOVIDO4', 32)

            base, ext = os.path.splitext(fileName)
            if "_" in base:
                base = base.rsplit("_", 1)[0]
            nome_final = base + ext
            final_path = os.path.join(DIRLIDOS, nome_final)

            if not os.path.exists(final_path):
                os.rename(destino, final_path)

            if not os.path.exists(os.path.join(DIRLIDOS, nome_final)):
                print_color(f"[ERRO CRÍTICO] Arquivo não encontrado em LIDOS após processamento: {fileName}", 31)

                if roomIds is not None and not ja_enviou_element:
                    msgElement = (
                        f"🚨 ERRO CRÍTICO PÓS-GRAVAÇÃO 🚨\n\n"
                        f"Arquivo: {fileName}\n"
                        f"Unidade: {Unidade} - {NomeUnidade}\n"
                        f"Conta: {AccountIdentifier}\n\n"
                        f"O banco confirmou gravação, mas o ZIP NÃO foi movido para a pasta LIDOS.\n"
                        f"Verificar imediatamente possível inconsistência de filesystem."
                    )
                    for roomId in roomIds:
                        elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                        print_color(f"{elementLog}", 33)
                        time.sleep(2)
                    ja_enviou_element = True
    else:
        filePath = DIRERROS + fileName

        if roomIds is not None and not ja_enviou_element:
            msgElement = build_element_message(
                fileName=fileName,
                Unidade=Unidade,
                NomeUnidade=NomeUnidade,
                AccountIdentifier=AccountIdentifier,
                type=retornoJson.get('type') if isinstance(retornoJson, dict) else 'N/A',
                retorno=retornoJson
            )

            # Para ver no terminal a mensagem enviada para o Element
            print_color(f"\nEnvio da Mensagem Element:\n{msgElement}", 32)

            for roomId in roomIds:
                elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                print_color(f"{elementLog}", 33)
                time.sleep(2)
            ja_enviou_element = True

        if not os.path.exists(filePath):
            shutil.move(source, DIRERROS)
            print_color('MOVIDO5', 32)

            # Novo nome do arquivo
            new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

            # Renomeia o arquivo
            os.rename(filePath, new_filename)
        else:
            os.remove(source)

        removeFolderFiles(folderZip)

def exibirRetonoPython(returno, Unidade, fileName, AccountIdentifier, folderZip, source, NomeUnidade, flagDados, roomIds):

    if not returno['BANCO']:
        if roomIds is not None:
            msgElement = f"ERRO DE PROCESSAMENTO ARQUIVO WHATSAPP {fileName}"

            print_color(f"\nEnvio da Mensagem {msgElement}", 33)

            for roomId in roomIds:
                elementLog = sendMessageElement(ACCESSTOKEN, roomId[0], msgElement)
                print_color(f"{elementLog}", 33)
                time.sleep(2)

        print_color(
            f"\nERRO GRAVAÇÃO NO BANCO DE DADOS(py)!!! {fileName} Unidade {Unidade} {NomeUnidade}",
            31)

        filePath = DIRERROS + fileName

        if not os.path.exists(filePath):
            shutil.move(source, DIRERROS)
            print_color('MOVIDO6', 32)

            # Novo nome do arquivo
            new_filename = filePath.replace('.zip', f'_{Unidade}.zip')

            # Renomeia o arquivo
            os.rename(filePath, new_filename)
        else:
            os.remove(source)

        removeFolderFiles(folderZip)
    else:
        removeFolderFiles(folderZip)

        # if flagDados:
        #     saveResponse(AccountIdentifier, Unidade)

        filePath = DIRLIDOS + fileName

        if not os.path.exists(filePath):
            shutil.move(source, DIRLIDOS)
            print_color('MOVIDO7', 32)
        else:
            delete_log(source)
            print_color(
                f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade} {NomeUnidade}",
                32)

def atualizar_conta_zap(conn):
    """
    Atualiza conta_zap com base em conta_id (varchar),
    extraindo apenas os dígitos e validando valor > 0.
    """
    # print_color("\n Normalizando conta_zap == NULL...", 32)

    sql_update = "UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = regexp_replace(conta_id, '[^0-9]', '', 'g')::bigint WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL AND conta_id IS NOT NULL AND regexp_replace(conta_id, '[^0-9]', '', 'g') <> '' AND regexp_replace(conta_id, '[^0-9]', '', 'g')::bigint > 0;"

    try:
        with conn.cursor() as cur:
            # print(" Executando UPDATE em lote com normalização...")
            cur.execute(sql_update)

            linhas = cur.rowcount
            conn.commit()

            if linhas != 0:
                print_color(f"      {linhas} - conta_zap normalizada.",32)
            # else:
            #     print_color("       Nenhuma conta_zap == NULL para normalização.",33)

    except Exception as e:
        conn.rollback()
        print(" Erro ao normalizar conta_zap.")
        print(f" Detalhes: {e}")
        raise

if __name__ == '__main__':
    checkFolder(DIRNOVOS)
    checkFolder(DIRLIDOS)
    checkFolder(DIRERROS)
    checkFolder(DIREXTRACAO)
    checkFolder(DIRLOG)

    previous_files = get_files_in_dir(DIRNOVOS)

    atualizar_conta_zap(conn) #atualiza coluna tbaplicativo_linhafone.conta_zap que for ativo e estiver null
    conn.close()

    dttmpstatus = ""
    print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads V7.1_ 05/02/2026 - Banco: {DB_HOST}\n")

    while True:
        time.sleep(3)
        current_files = get_files_in_dir(DIRNOVOS)
        added_files = current_files - previous_files
        removed_files = previous_files - current_files

        try:

            # if added_files:
            #     for file in added_files:
            #         full_path = os.path.join(DIRNOVOS, file)
            #         if os.path.isdir(full_path):
            #             print(f"Ignorando diretório: {file}")
            #             continue
            #         if not file.lower().endswith('.zip'):
            #             print(f"Ignorando não-zip: {file}")
            #             continue
            #         process(full_path)
            if added_files:
                # 1) separar em DADOS_ e não-DADOS_
                dados_zips = sorted(
                    f for f in added_files
                    if f.lower().endswith('.zip') and f.upper().startswith('DADOS_')
                )
                other_zips = sorted(
                    f for f in added_files
                    if f.lower().endswith('.zip') and not f.upper().startswith('DADOS_')
                )
                # 2) concatena para garantir a ordem desejada
                ordered_files = dados_zips + other_zips

                for file in ordered_files:
                    full_path = os.path.join(DIRNOVOS, file)
                    # mantém sua lógica de ignorar diretórios e não-zip
                    if os.path.isdir(full_path):
                        print(f"Ignorando diretório: {file}")
                        continue
                    if not file.lower().endswith('.zip'):
                        print(f"Ignorando não-zip: {file}")
                        continue
                    # processa na ordem: primeiro DADOS_* depois os outros
                    print(f"Processando: {file}")
                    process(full_path)

            if removed_files:
                print(f'\nArquivos removidos: {removed_files}')
        except Exception as inst:
            print("ERRO GERAL:", inst)
            traceback.print_exc()

        previous_files = current_files

        result = StatusServidor(dttmpstatus)
        if result == True:
            dttmpstatus = datetime.today().strftime('%Y%m%d%H%M%S')
        else:
            printTimeData()