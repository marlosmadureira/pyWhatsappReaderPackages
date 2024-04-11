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
    removeFolderFiles, print_color, somentenumero, grava_log, delete_log, openJsonEstruturado, contar_arquivos_zip, \
    get_size, getUnidadeFileName, tipoHtml
from pyGetSendApi import sendDataJsonServer, setDateObjetoProrrogue
from pyPostgresql import sendDataPostgres
from pyRequestParameter import requestReaderParameter
from pyPRTT import message_logReader, call_logsReader
from pyDados import book_infoReader, groups_infoReader, ncmec_reportsReader, connection_infoReader, web_infoReader, \
    emails_infoReader, ip_addresses_infoReader, small_medium_business_infoReader, device_infoReader

# Configs
load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")

DebugMode = False
Out = True
Executar = False


class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.zip"]

    def process(self, event):

        if DebugMode:
            print("\nLog Evento:" + str(event))

        countdown(2)

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

                    tag1, tag2, tag3, tag4 = tipoHtml(request_parameters)

                    parameter = requestReaderParameter(request_parameters, DebugMode, Out, tag1, tag2, tag3, tag4)

                    if parameter is not None:

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

                        if contaZap is not None:
                            setDateObjetoProrrogue(contaZap, Unidade, fileName)

                            message_log = bsHtml.find('div', attrs={"id": "property-message_log"})
                            call_logs = bsHtml.find('div', attrs={"id": "property-call_logs"})

                            if message_log is not None:
                                dataType = "PRTT"

                                messages = message_logReader(message_log, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                calls = call_logsReader(call_logs, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)

                                if messages is not None:
                                    fileDados['msgLogs'] = messages

                                if calls is not None:
                                    fileDados['callLogs'] = calls

                                if 'PRTT' in dataType:
                                    fileProcess['Prtt'] = fileDados

                            ncmec_reports = bsHtml.find('div', attrs={"id": "property-ncmec_reports"})
                            emails_info = bsHtml.find('div', attrs={'id': "property-emails"})
                            ip_addresses_info = bsHtml.find('div', attrs={"id": "property-ip_addresses"})
                            connection_info = bsHtml.find('div', attrs={"id": "property-connection_info"})
                            web_info = bsHtml.find('div', attrs={"id": "property-web_info"})
                            groups_info = bsHtml.find('div', attrs={"id": "property-groups_info"})
                            address_book_info = bsHtml.find('div', attrs={"id": "property-address_book_info"})
                            small_medium_business_info = bsHtml.find('div',
                                                                     attrs={"id": "property-small_medium_business"})
                            device_info = bsHtml.find('div', attrs={"id": "property-device_info"})

                            if address_book_info is not None:
                                dataType = "DADOS"

                                emailsinfo = emails_infoReader(emails_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if emailsinfo is not None:
                                    fileDados['EmailAddresses'] = emailsinfo

                                ipaddresses = ip_addresses_infoReader(ip_addresses_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if ipaddresses is not None:
                                    fileDados['ipAddresses'] = ipaddresses

                                bookinfo = book_infoReader(address_book_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if address_book_info is not None:
                                    fileDados['addressBookInfo'] = bookinfo

                                groupsinfo = groups_infoReader(groups_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if groups_info is not None:
                                    fileDados['groupsInfo'] = groupsinfo

                                ncmecreports = ncmec_reportsReader(ncmec_reports, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if ncmecreports is not None:
                                    fileDados['ncmecReportsInfo'] = ncmecreports

                                connectioninfo = connection_infoReader(connection_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if connection_info is not None:
                                    fileDados['connectionInfo'] = connectioninfo

                                webinfo = web_infoReader(web_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if web_info is not None:
                                    fileDados['webInfo'] = webinfo

                                smallmediumbusinessinfo = small_medium_business_infoReader(small_medium_business_info,
                                                                                           fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if smallmediumbusinessinfo is not None:
                                    fileDados['smallmediumbusinessinfo'] = smallmediumbusinessinfo

                                deviceinfo = device_infoReader(device_info, fileName, DebugMode, Out, tag1, tag2, tag3, tag4)
                                if deviceinfo is not None:
                                    fileDados['deviceinfo'] = deviceinfo

                                if 'DADOS' in dataType:
                                    fileProcess['Dados'] = fileDados

                            print_color(f"\n{dataType}", 31)

                            if Executar:
                                sizeFile = get_size(source)

                                print_color(f"\nTAMANHO DO PACOTE {sizeFile}", 32)

                                if sizeFile > 400000:
                                    print_color(
                                        f"\n=========================== PYTHON {fileName} Unidade {Unidade} ===========================",
                                        32)

                                    sendDataPostgres(fileProcess, dataType, DebugMode, Out, fileName)
                                else:
                                    print_color(
                                        f"\n=========================== ENVIADO PHP {fileName} Unidade {Unidade} ===========================",
                                        32)

                                    retornoJson = sendDataJsonServer(fileProcess, dataType)

                                    if 'MostraJsonPython' in retornoJson['jsonRetorno']:

                                        Jsondata = json.loads(retornoJson['jsonRetorno'])

                                        if Jsondata['MostraJsonPython']:
                                            openJsonEstruturado(fileProcess)

                                        if Jsondata['ExibirTotalPacotesFila']:
                                            contar_arquivos_zip(DIRNOVOS)

                                        if Jsondata['GravaBanco']:
                                            print_color(f"\nGRAVOU COM SUCESSO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade}",32)
                                        else:
                                            print_color(f"\nERRO GRAVAÇÃO NO BANCO DE DADOS!!! {fileName} Unidade {Unidade}",32)

                                    print(f"\n{retornoJson}")

                            else:
                                print_color(
                                    f"\n================= ENVIO PHP/PYTHON DESLIGADO {fileName} Unidade {Unidade} =================",
                                    31)

                                grava_log(fileProcess, f'Log_{dataType}_Out{fileName}.json')

                            removeFolderFiles(folderZip)

                            filePath = DIRLIDOS + fileName

                            if not os.path.exists(filePath):
                                shutil.move(source, DIRLIDOS)
                            else:
                                delete_log(source)

                            print_color(f"\nFim {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", 35)
                        else:
                            print_color(f"Erro de Padrão HTML {parameter}", 31)

                            grava_log(f"Erro de Padrão HTML: {fileName} Unidade: {Unidade}",
                                      'LogPadraoAntigo.txt')

                            removeFolderFiles(folderZip)

                            filePath = DIRLIDOS + fileName

                            if not os.path.exists(filePath):
                                shutil.move(source, DIRERROS)
                            else:
                                delete_log(source)
                else:
                    print_color(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 31)

                    grava_log(f"Erro Arquivo Contém Index: {fileName} Unidade: {Unidade}", 'LogPadraoAntigo.txt')

                    # removeFolderFiles(folderZip)
                    #
                    # filePath = DIRLIDOS + fileName
                    #
                    # if not os.path.exists(filePath):
                    #     shutil.move(source, DIRERROS)
                    # else:
                    #     delete_log(source)

            except Exception as inst:

                print_color(f"Location: process - Files Open, error: {str(inst)} File: {str(source)}", 31)

                # delete_log(f'log/Log_Error_{dataType}_Out_{fileName}.json')

                # grava_log(fileProcess, f'Log_Error_{dataType}_Out_{fileName}.json')

                filePath = DIRERROS + fileName

                if not os.path.exists(filePath):
                    shutil.move(source, DIRERROS)
                else:
                    os.remove(source)

            if DebugMode:
                print("\nMovendo de: ", source)
                print("Para: ", DIRLIDOS)
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
        print_color(f"\n{event.src_path} foi deletado!", 36)

    def on_modified(self, event):
        print_color(f"\n{event.src_path} foi modificado!", 36)

    def on_moved(self, event):
        print_color(f"\n{event.src_path} foi movido/renomeado para {event.dest_path}!", 36)


if __name__ == '__main__':
    checkFolder(DIRNOVOS)
    checkFolder(DIRLIDOS)
    checkFolder(DIRERROS)
    checkFolder(DIREXTRACAO)
    checkFolder("log")

    dttmpstatus = ""
    print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

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
