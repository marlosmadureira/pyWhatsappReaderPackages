# -*- coding: utf-8 -*-
# !/usr/bin/python3

import os
import time
import shutil

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBiblioteca import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parseHTMLFile, removeFolderFiles
from pyFindPostgresql import setDateObjetoProrrogue
from pyRequestParameter import requestParameter
from pyPRTT import processPrttFile
from pyDados import processDadosFile

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")

DebugMode = True


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

            fileProcess = {}
            data = {}

            try:
                fileName = source.replace(DIRNOVOS, "")
                folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
                bsHtml = parseHTMLFile(folderZip)

                if bsHtml is not None and bsHtml != "":
                    print('Inicio Leitura HTML ', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

                    dataType = None

                    request_parameters = bsHtml.find('div', attrs={"id": "property-request_parameters"})
                    message_log = bsHtml.find('div', attrs={"id": "property-message_log"})
                    call_logs = bsHtml.find('div', attrs={"id": "property-call_logs"})

                    if request_parameters is not None and request_parameters != "" and len(request_parameters) == 6:
                        parameter = requestParameter(request_parameters)

                    if message_log is not None and message_log != "":
                        print(f"{message_log}")

                    if call_logs is not None and call_logs != "":
                        print(f"{call_logs}")

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
