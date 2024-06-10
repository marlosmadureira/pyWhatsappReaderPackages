# -*- coding: utf-8 -*-
# !/usr/bin/python3
import json
import os
import re

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBibliotecaV3 import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, print_color, \
    parsetHTLMFileString, grava_log, getUnidadeFileName, removeFolderFiles, delete_log
from pyPostgresql import find_unidade_postgres

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIREXTRACAO = os.getenv("DIREXTRACAO")

DebugMode = False
Out = True
Executar = True


class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.zip"]

    def process(self, event):

        if DebugMode:
            print("\nLog Evento:" + str(event))

        countdown(1)

        if event.event_type == "created":

            fileProcess = {}
            fileDados = {}

            datamovimento = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            source = event.src_path

            source, Unidade = getUnidadeFileName(source)

            if DebugMode:
                print("Evento: " + event.src_path, event.event_type)  # print now only for degug

                printDebug(" Iniciando arquivo: " + str(source) + " - em: " + str(datamovimento) + "\n")

            fileName = source.replace(DIRNOVOS, "")
            folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
            bsHtml = parsetHTLMFileString(folderZip)

            pathDeleteZipUploads = f"{os.getcwd()}/{DIRNOVOS}{os.path.splitext(fileName)[0]}"
            pathDeleteExtracao = f"{os.getcwd()}/{DIREXTRACAO}{os.path.splitext(fileName)[0]}"

            if bsHtml is not None and bsHtml != "" and Unidade is not None:

                NomeUnidade = find_unidade_postgres(Unidade)

                # print(f"{bsHtml}")

                # parsed_json_parameters = parse_dynamic_sentence_parameters(bsHtml)
                # print(f"\n{parsed_json_parameters}")
                #
                # parsed_json_books = parse_dynamic_sentence_books(bsHtml)
                # print(f"\n{parsed_json_books}")
                #
                # parsed_json_ip_addresses = parse_dynamic_sentence_ip_addresses(bsHtml)
                # print(f"\n{parsed_json_ip_addresses}")
                #
                # parsed_json_connection = parse_dynamic_sentence_connection(bsHtml)
                # print(f"\n{parsed_json_connection}")
                #
                # parsed_json_device = parse_dynamic_sentence_device(bsHtml)
                # print(f"\n{parsed_json_device}")
                #
                # parsed_json_group = parse_dynamic_sentence_group(bsHtml)
                # print(f"\n{parsed_json_group}")

                parsed_json_web = parse_dynamic_sentence_web(bsHtml)
                print(f"\n{parsed_json_web}")

                parsed_json_small = parse_dynamic_sentence_small(bsHtml)
                print(f"\n{parsed_json_small}")

                parsed_json_message = parse_dynamic_sentence_message(bsHtml)
                print(f"\n{parsed_json_message}")

            else:
                print_color(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 31)

                grava_log(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 'LogPadraoAntigo.txt')

            if os.path.exists(pathDeleteZipUploads):
                removeFolderFiles(pathDeleteZipUploads)

            if os.path.exists(pathDeleteExtracao):
                removeFolderFiles(pathDeleteExtracao)

            if os.path.exists(source):
                delete_log(source)

            if DebugMode:
                print("\nMovendo de: ", source)
                print("Arquivo Finalizado!\n")
                print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads")
            else:
                print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads")

    def on_created(self, event):
        try:
            self.process(event)
        except Exception as inst:
            pass

        print_color(f"\n{event.src_path} foi criado!", 36)

    def on_deleted(self, event):
        try:
            print_color(f"\n{event.src_path} foi deletado!", 36)
        except Exception as inst:
            pass

    def on_modified(self, event):
        try:
            print_color(f"\n{event.src_path} foi modificado!", 36)
        except Exception as inst:
            pass

    def on_moved(self, event):
        try:
            print_color(f"\n{event.src_path} foi movido/renomeado para {event.dest_path}!", 36)
        except Exception as inst:
            pass


def parse_dynamic_sentence_parameters(sentence):
    # Expressões regulares para capturar os diferentes campos
    patterns = {
        "Service": r"Service(\w+)",
        "Internal Ticket Number": r"Internal Ticket Number(\d+)",
        "Account Identifier": r"Account Identifier(\+\d+)",
        "Account Type": r"Account Type(\w+)",
        "User Generated": r"Generated(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Date Range": r"Date Range(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC to \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
        "Ncmec Reports Definition": r"Ncmec Reports Definition(NCMEC Reports: [\w\s\(\)]+)",
        "NCMEC CyberTip Numbers": r"NCMEC CyberTip Numbers([\w\s]+)",
        "Emails Definition": r"Emails Definition(Emails: [\w\s':]+)",
        "Registered Email Addresses": r"Registered Email Addresses([\w\s]+)"
    }

    # Dicionário para armazenar os resultados
    result = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            result[key] = match.group(1).strip()

    if len(result) > 0:
        print("\nRequest Parameters")
        return json.dumps(result, indent=4)
    else:
        return None


def parse_dynamic_sentence_books(sentence):
    allRegistros = []

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

    data['Symmetric'] = symmetric_numbers
    data['Asymmetric'] = asymmetric_numbers

    allRegistros.append(data)

    if len(data['Symmetric']) > 0 or len(data['Asymmetric']) > 0:
        print("\nAddress Book Info")
        return allRegistros
    else:
        return None


def parse_dynamic_sentence_ip_addresses(sentence):
    # Expressão regular para capturar os pares de "Time" e "IP Address"
    time_ip_pattern = re.compile(r"Time(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\s+IP Address([0-9a-fA-F\.:]+)")

    # Capturar todos os pares de "Time" e "IP Address" da frase
    time_ip_matches = time_ip_pattern.findall(sentence)

    # Criar uma lista de dicionários para as conexões
    connections = [{"Time": time, "IP Address": ip} for time, ip in time_ip_matches]

    if len(connections) > 0:
        print("\nIp Addresses")
        connections_json = json.dumps(connections, indent=4)
        return connections_json
    else:
        return None


def parse_dynamic_sentence_connection(sentence):
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
    result = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            result[key] = match.group(1).strip() if key != "Push Name" else match.group(1).strip()

    if len(result) > 0:
        print("\nConnection")
        connection_info_json = json.dumps(result, indent=4)
        return connection_info_json
    else:
        return None


def parse_dynamic_sentence_device(sentence):
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
    result = {}

    # Iterar sobre os padrões e encontrar as correspondências
    for key, pattern in patterns.items():
        match = re.search(pattern, sentence)
        if match:
            result[key] = match.group(1).strip()

    if len(result) > 0:
        print("\nDevice")
        device_info_json = json.dumps(result, indent=4)
        return device_info_json
    else:
        return None


def parse_dynamic_sentence_group(sentence):
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
    if owned_section:
        owned_section_text = owned_section.group(0)
        owned_group = {}
        for key, pattern in group_patterns.items():
            match = re.search(pattern, owned_section_text)
            if match:
                owned_group[key] = match.group(1).strip()
        owned_groups.append(owned_group)

    # Capturar informações dos grupos Participating
    participating_section = re.search(r"GroupsParticipating.*$", sentence, re.DOTALL)
    if participating_section:
        participating_section_text = participating_section.group(0)
        participating_group = {}
        for key, pattern in group_patterns.items():
            match = re.search(pattern, participating_section_text)
            if match:
                participating_group[key] = match.group(1).strip()
        participating_groups.append(participating_group)

    # Formatar os resultados como JSON
    result = {
        "Owned": owned_groups,
        "Participating": participating_groups
    }

    if len(owned_groups) > 0 or len(participating_groups) > 0:
        print("\nGroup")
        groups_info_json = json.dumps(result, indent=4)
        return groups_info_json
    else:
        return None


def parse_dynamic_sentence_web(sentence):
    print(f"FALTA FAZER")


def parse_dynamic_sentence_small(sentence):
    print(f"FALTA FAZER")


def parse_dynamic_sentence_message(sentence):
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
        result = {}
        # Iterar sobre os padrões e encontrar as correspondências
        for key, pattern in message_patterns.items():
            match = re.search(pattern, message)
            if match:
                result[key] = match.group(1).strip()
        # Adicionar apenas se result não estiver vazio
        if result:
            results.append(result)

    if len(results) > 0:
        print("\nMessage")
        groups_info_json = json.dumps(results, indent=4)
        return groups_info_json
    else:
        return None


if __name__ == '__main__':
    checkFolder(DIRNOVOS)
    checkFolder(DIREXTRACAO)
    checkFolder("log")

    dttmpstatus = ""
    print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

    observer = Observer()
    observer.schedule(MyHandler(), path=DIRNOVOS if DIRNOVOS else '.')
    observer.start()

    try:
        while True:
            result = StatusServidor(dttmpstatus)
            if result == True:
                dttmpstatus = datetime.today().strftime('%Y%m%d%H%M%S')
            else:
                printTimeData()
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
