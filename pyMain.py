# -*- coding: utf-8 -*-
# !/usr/bin/python3
import json
import os
import time
import shutil

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBiblioteca import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parseHTMLFile, \
    removeFolderFiles, print_color, somentenumero, openJsonEstruturado
from pyFindApi import sendDataJsonServer, setDateObjetoProrrogue
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
Executar = False


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

        if DebugMode:
            print("\nLog Evento:" + str(event))

        countdown(2)
        print('\n')

        if event.event_type == "created":

            fileProcess = {}
            fileDados = {}

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

                dataType = None

                fileProcess['FileName'] = fileName
                fileProcess['Unidade'] = Unidade

                if bsHtml is not None and bsHtml != "":

                    print_color(f"Inicio Leitura HTML {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", 35)

                    print_color(f"\nOUT {fileName}", 34)

                    # Cabeçalho de Todos os Arquivos HTML
                    request_parameters = bsHtml.find('div', attrs={"id": "property-request_parameters"})
                    parameter = requestReaderParameter(request_parameters, DebugMode)

                    contaZap = None
                    if 'Service' in parameter:
                        fileProcess['Service'] = parameter['Service']
                    if 'InternalTicketNumber' in parameter:
                        fileProcess['InternalTicketNumber'] = parameter['InternalTicketNumber']
                    if 'AccountType' in parameter:
                        contaZap = somentenumero(parameter['AccountIdentifier'])
                        fileProcess['AccountIdentifier'] = contaZap
                    if 'AccountType' in parameter:
                        fileProcess['AccountType'] = parameter['AccountType']
                    if 'Generated' in parameter:
                        fileProcess['Generated'] = parameter['Generated']
                    if 'DateRange' in parameter:
                        fileProcess['DateRange'] = parameter['DateRange']
                    if 'RegisteredEmailAddresses' in parameter:
                        fileProcess['EmailAddresses'] = parameter['RegisteredEmailAddresses']

                    if contaZap is not None:
                        setDateObjetoProrrogue(contaZap, Unidade, fileName)

                    message_log = bsHtml.find('div', attrs={"id": "property-message_log"})
                    call_logs = bsHtml.find('div', attrs={"id": "property-call_logs"})

                    if message_log is not None or call_logs is not None:
                        dataType = "PRTT"

                        if message_log is not None and message_log != "":
                            messages = message_logReader(message_log, fileName, DebugMode)

                        if call_logs is not None and call_logs != "":
                            calls = call_logsReader(call_logs, fileName, DebugMode)

                        if 'PRTT' in dataType:
                            fileDados['msgLogs'] = messages
                            fileDados['callLogs'] = calls
                            fileProcess['Prtt'] = fileDados

                    address_book_info = bsHtml.find('div', attrs={"id": "property-address_book_info"})
                    groups_info = bsHtml.find('div', attrs={"id": "property-groups_info"})
                    ncmec_reports = bsHtml.find('div', attrs={"id": "property-ncmec_reports"})
                    connection_info = bsHtml.find('div', attrs={"id": "property-connection_info"})
                    web_info = bsHtml.find('div', attrs={"id": "property-web_info"})

                    if address_book_info is not None or groups_info is not None or ncmec_reports is not None or ncmec_reports is not None or connection_info is not None or web_info is not None:
                        dataType = "DADOS"

                        bookinfo = book_infoReader(address_book_info, fileName, DebugMode)
                        if address_book_info is not None:
                            fileDados['addressBookInfo'] = bookinfo

                        groupsinfo = groups_infoReader(groups_info, fileName, DebugMode)
                        if groups_info is not None:
                            fileDados['groupsInfo'] = groupsinfo

                        ncmecreports = ncmec_reportsReader(ncmec_reports, fileName, DebugMode)
                        if ncmecreports is not None:
                            fileDados['ncmecReportsInfo'] = ncmecreports

                        connectioninfo = connection_infoReader(connection_info, fileName, DebugMode)
                        if connection_info is not None:
                            fileDados['connectionInfo'] = connectioninfo

                        webinfo = web_infoReader(web_info, fileName, DebugMode)
                        if web_info is not None:
                            fileDados['webInfo'] = webinfo

                        print_color(f"\n{dataType}", 31)

                        if 'DADOS' in dataType:
                            fileProcess['Dados'] = fileDados

                    print_color(f"\nFim {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", 35)

                    print('\nEnvio PHP ', datetime.now().strftime('%d/%m/%Y %H:%M:%S'), '\n')

                    if Executar:
                        sendDataJsonServer(fileProcess, dataType)

                    print('\nFim ', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

                    removeFolderFiles(folderZip)

                    filePath = DIRLIDOS + fileName

                    if not os.path.exists(filePath):
                        shutil.move(source, DIRLIDOS)
                    else:
                        os.remove(source)

            except Exception as inst:
                print_color(f"Location: process - Files Open, error: {str(inst)} File: {str(source)}", 31)
                pass

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
