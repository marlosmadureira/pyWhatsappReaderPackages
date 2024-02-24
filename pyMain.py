# -*- coding: utf-8 -*-
# !/usr/bin/python3

import os
import time
import shutil

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBiblioteca import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parseHTMLFile, removeFolderFiles, print_color
from pyFindPostgresql import setDateObjetoProrrogue
from pyRequestParameter import requestReaderParameter
from pyPRTT import message_logReader, call_logsReader
from pyDados import book_infoReader, groups_infoReader, ncmec_reportsReader, connection_infoReader, web_infoReader

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")

DebugMode = False


class CallEventsDict:
    def __init__(self, type=None, timestamp=None, solicitante=None, atendente=None, solIP=None, solPort=None,
                 mediaType=None, Participants=None):
        self.type = type
        self.timestamp = timestamp
        self.solicitante = solicitante
        self.atendente = atendente
        self.solIP = solIP
        self.solPort = solPort
        self.mediaType = mediaType
        self.Participants = Participants


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


class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.zip"]

    def process(self, event):

        LocalDumpBD = False

        if DebugMode:
            print("\nLog Evento:" + str(event))

        countdown(3)
        print('\n')

        if event.event_type == "created":
            datamovimento = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            source = event.src_path

            source, Unidade = getUnidadeFileName(source)

            if DebugMode:
                print("Evento: " + event.src_path, event.event_type)  # print now only for degug
                printDebug(" Iniciando arquivo: " + str(source) + " - em: " + str(datamovimento) + "\n")

            try:
                fileName = source.replace(DIRNOVOS, "")
                folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
                bsHtml = parseHTMLFile(folderZip)

                if bsHtml is not None and bsHtml != "":
                    print('Inicio Leitura HTML ', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

                    dataType = None

                    request_parameters = bsHtml.find('div', attrs={"id": "property-request_parameters"})
                    parameter = requestReaderParameter(request_parameters, DebugMode)

                    if DebugMode:
                        print(f"{bsHtml}")

                    message_log = bsHtml.find('div', attrs={"id": "property-message_log"})
                    call_logs = bsHtml.find('div', attrs={"id": "property-call_logs"})

                    if message_log is not None and message_log != "":
                        messages = message_logReader(message_log, DebugMode)

                    if call_logs is not None and call_logs != "":
                        calls = call_logsReader(call_logs, DebugMode)

                    address_book_info = bsHtml.find('div', attrs={"id": "property-address_book_info"})
                    groups_info = bsHtml.find('div', attrs={"id": "property-groups_info"})
                    ncmec_reports = bsHtml.find('div', attrs={"id": "property-ncmec_reports"})
                    connection_info = bsHtml.find('div', attrs={"id": "property-connection_info"})
                    web_info = bsHtml.find('div', attrs={"id": "property-web_info"})

                    if address_book_info is not None:
                        book_info = book_infoReader(address_book_info, DebugMode)

                    if groups_info is not None:
                        groups_info = groups_infoReader(groups_info, DebugMode)

                    if ncmec_reports is not None:
                        ncmec_reports = ncmec_reportsReader(ncmec_reports, DebugMode)

                    if connection_info is not None:
                        connection_info = connection_infoReader(connection_info, DebugMode)

                    if web_info is not None:
                        web_info = web_infoReader(web_info, DebugMode)

                    print('\nFim ', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

                    removeFolderFiles(folderZip)

                    filePath = DIRLIDOS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRLIDOS)
                    else:
                        os.remove(source)

            except Exception as inst:
                errorData = "{Location: process - Files Open, error: " + str(inst) + ", File: " + str(source) + "}"
                # sendSlackMSG(errorData)

                filePath = DIRERROS + fileName

                if not os.path.exists(filePath):
                    shutil.move(source, DIRERROS)
                else:
                    os.remove(source)

            if DebugMode:
                print("\nMovendo de: ", source)
                print("Para: ", DIRLIDOS)
                print("Arquivo Finalizado!\n")
                print("\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")
            else:
                print("\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':
    dttmpstatus = ""
    print("\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

    checkFolder(DIRNOVOS)
    checkFolder(DIRLIDOS)
    checkFolder(DIRERROS)
    checkFolder(DIREXTRACAO)

    observer = Observer()
    observer.schedule(MyHandler(), path=DIRNOVOS if DIRNOVOS else '.')
    observer.start()

    try:
        while True:
            time.sleep(5)
            result = StatusServidor(dttmpstatus)
            if result == True:
                dttmpstatus = datetime.today().strftime('%Y%m%d%H%M%S')
            else:
                printTimeData()
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
