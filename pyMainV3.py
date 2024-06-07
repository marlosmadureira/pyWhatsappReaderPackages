# -*- coding: utf-8 -*-
# !/usr/bin/python3
import os

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBiblioteca import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parse_dynamic_sentence_parameters, \
    print_color, parsetHTLMFileString, grava_log, getUnidadeFileName, removeFolderFiles, delete_log
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

                print(f"\n{bsHtml}")

                parsed_json = parse_dynamic_sentence_parameters(bsHtml)

                print(f"\n{parsed_json}")

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
