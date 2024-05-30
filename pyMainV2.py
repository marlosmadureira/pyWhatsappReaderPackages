# -*- coding: utf-8 -*-
# !/usr/bin/python3
import os

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBiblioteca import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parseHTMLFile, \
    print_color, somentenumero, grava_log, getUnidadeFileName, removeFolderFiles, delete_log
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

            datamovimento = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            source = event.src_path

            source, Unidade = getUnidadeFileName(source)

            if DebugMode:
                print("Evento: " + event.src_path, event.event_type)  # print now only for degug

                printDebug(" Iniciando arquivo: " + str(source) + " - em: " + str(datamovimento) + "\n")

            fileName = source.replace(DIRNOVOS, "")
            folderZip = unzipBase(source, DIRNOVOS, DIREXTRACAO)
            bsHtml = parseHTMLFile(folderZip)

            pathDeleteZipUploads = f"{os.getcwd()}/{DIRNOVOS}{os.path.splitext(fileName)[0]}"
            pathDeleteExtracao = f"{os.getcwd()}/{DIREXTRACAO}{os.path.splitext(fileName)[0]}"

            if bsHtml is not None and bsHtml != "" and Unidade is not None:

                NomeUnidade = find_unidade_postgres(Unidade)

                readHeader(bsHtml)
                # DADOS
                readGroup(bsHtml)
                readBook(bsHtml)
                # PRTT
                #readMessageLogs(bsHtml)
                readCallLogs(bsHtml)

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
        print_color(f"\n{event.src_path} foi deletado!", 36)

    def on_modified(self, event):
        print_color(f"\n{event.src_path} foi modificado!", 36)

    def on_moved(self, event):
        print_color(f"\n{event.src_path} foi movido/renomeado para {event.dest_path}!", 36)


def readHeader(bsHtml):
    print("\nHeader Info")
    header = {}

    service = bsHtml.find(text="Service")
    internal_ticket_number = bsHtml.find(text="Internal Ticket Number")
    account_identifier = bsHtml.find(text="Account Identifier")
    account_type = bsHtml.find(text="Account Type")
    generated = bsHtml.find(text="Generated")
    date_range = bsHtml.find(text="Date Range")
    ncmec_reports_definition = bsHtml.find(text="Ncmec Reports Definition")
    ncmec_cybertip_numbers = bsHtml.find(text="NCMEC CyberTip Numbers")

    if service:
        service_info = service.find_next().text.strip()
        header['service_info'] = service_info

    if internal_ticket_number:
        internal_ticket_number_info = somentenumero(internal_ticket_number.find_next().text)
        header['internal_ticket_number_info'] = internal_ticket_number_info

    if account_identifier:
        account_identifier_info = somentenumero(account_identifier.find_next().text)
        header['account_identifier_info'] = account_identifier_info

    if account_type:
        account_type_info = account_type.find_next().text.strip()
        header['account_type_info'] = account_type_info

    if generated:
        generated_info = generated.find_next().text.strip()
        header['generated_info'] = generated_info

    if date_range:
        date_range_info = date_range.find_next().text
        header['date_range_info'] = date_range_info

    if ncmec_reports_definition:
        ncmec_reports_definition_info = ncmec_reports_definition.find_next().text
        header['ncmec_reports_definition_info'] = ncmec_reports_definition_info

    if ncmec_cybertip_numbers:
        ncmec_cybertip_numbers_info = ncmec_cybertip_numbers.find_next().text.strip()
        header['ncmec_cybertip_numbers_info'] = ncmec_cybertip_numbers_info

    print(f"{header}")


def readGroup(bsHtml):
    # Encontrar a seção correspondente às imagens ("Picture")
    print("\nGroup Info")
    pictures = []
    picture_sections = bsHtml.find_all(text='Picture')

    if picture_sections:
        for section in picture_sections:
            picture_data = {}

            # Navegar para os próximos elementos para extrair os dados
            linked_media_file = section.find_next(text='Linked Media File:').find_next().text.strip()
            thumbnail = section.find_next(text='Thumbnail').find_next().text.strip()
            picture_id = section.find_next(text='ID').find_next().text.strip()
            creation_date = section.find_next(text='Creation').find_next().text.strip()
            size = section.find_next(text='Size').find_next().text.strip()

            picture_data['Linked Media File'] = linked_media_file
            picture_data['Thumbnail'] = thumbnail
            picture_data['ID'] = picture_id
            picture_data['Creation'] = creation_date
            picture_data['Size'] = size

            pictures.append(picture_data)

    # Exibir os dados extraídos
    for picture in pictures:
        print(picture)


def readBook(bsHtml):
    print("\nBook Info")

    # Função para extrair números de uma seção específica
    contacts = []
    sectionsSymmetric = bsHtml.find_all(text='Symmetric contacts')
    if sectionsSymmetric:
        for section in sectionsSymmetric:
            currentSymmetric = section.find_next()

            # Obter o texto dentro da tag <div>
            phone_text = currentSymmetric.get_text(separator='\n')
            # Dividir o texto em uma lista usando quebras de linha
            phone_list = phone_text.split('\n')

            print(f"{phone_list}")

    sectionsAsymmetric = bsHtml.find_all(text='Asymmetric contacts')
    if sectionsAsymmetric:
        for section in sectionsAsymmetric:
            currentAsymmetric = section.find_next()

            # Obter o texto dentro da tag <div>
            phone_text = currentAsymmetric.get_text(separator='\n')
            # Dividir o texto em uma lista usando quebras de linha
            phone_list = phone_text.split('\n')

            print(f"{phone_list}")


def readMessageLogs(bsHtml):
    # Encontrar todos os blocos que contêm a informação "Timestamp"
    print("\nMessage Log")
    message_blocks = bsHtml.find_all(text="Timestamp")

    if message_blocks:
        # Iterar sobre cada bloco e extrair as informações
        for block in message_blocks:
            menssage_data = {}
            timestamp = block.find_next().text.strip()
            message_id = block.find_next(text="Message Id").find_next().text.strip()
            sender = block.find_next(text="Sender").find_next().text.strip()
            recipients = block.find_next(text="Recipients").find_next().text.strip()
            sender_ip = block.find_next(text="Sender Ip").find_next().text.strip()
            sender_port = block.find_next(text="Sender Port").find_next().text.strip()
            sender_device = block.find_next(text="Sender Device").find_next().text.strip()
            msg_type = block.find_next(text="Type").find_next().text.strip()
            message_style = block.find_next(text="Message Style").find_next().text.strip()
            message_size = block.find_next(text="Message Size").find_next().text.strip()

            menssage_data['timestamp'] = timestamp
            menssage_data['message_id'] = message_id
            menssage_data['sender'] = sender
            menssage_data['recipients'] = recipients
            menssage_data['sender_ip'] = sender_ip
            menssage_data['sender_port'] = sender_port
            menssage_data['sender_device'] = sender_device
            menssage_data['msg_type'] = msg_type
            menssage_data['message_style'] = message_style
            menssage_data['message_size'] = message_size

            print(f"{menssage_data}")


def readCallLogs(bsHtml):
    # Encontrar todos os blocos que contêm a informação "Call"
    print("\nCall Logs")
    call_blocks = bsHtml.find_all(text='Call Id')

    if call_blocks:
        calls = []
        for call_block in call_blocks:
            call_data = {}

            # Encontrar o bloco pai que contém as informações da chamada
            parent = call_block.find_parent()

            call_data['Call Id'] = parent.find_next(text='Call Id').find_next().text.strip()
            call_data['Call Creator'] = parent.find_next(text='Call Creator').find_next().text.strip()

            events = []

            # Percorrer os eventos dentro do bloco de chamada
            current = parent.find_next(text='Events').find_next()

            while current and current.text.strip() not in ['Call Id', 'Events']:

                if 'Type' in current.text.strip():
                    event_data = {}
                    event_data['Type'] = current.find_next().text.strip()
                    event_data['Timestamp'] = current.find_next(text='Timestamp').find_next().text.strip()
                    event_data['From'] = current.find_next(text='From').find_next().text.strip()
                    event_data['To'] = current.find_next(text='To').find_next().text.strip()
                    event_data['From Ip'] = current.find_next(text='From Ip').find_next().text.strip()
                    event_data['From Port'] = current.find_next(text='From Port').find_next().text.strip()
                    event_data['Media Type'] = current.find_next(
                        text='Media Type').find_next().text.strip() if current.find_next(
                        text='Media Type') else 'N/A'
                    events.append(event_data)
                    current = current.find_next(text='Type')
                else:
                    current = current.find_next()

            call_data['Events'] = events
            calls.append(call_data)

        # Exibir os resultados
        for call in calls:
            print(f"Call Id: {call['Call Id']}")
            print(f"Call Creator: {call['Call Creator']}")
            for event in call['Events']:
                print(f"  Event Type: {event['Type']}")
                print(f"  Timestamp: {event['Timestamp']}")
                print(f"  From: {event['From']}")
                print(f"  To: {event['To']}")
                print(f"  From Ip: {event['From Ip']}")
                print(f"  From Port: {event['From Port']}")
                print(f"  Media Type: {event['Media Type']}")
            print()


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
