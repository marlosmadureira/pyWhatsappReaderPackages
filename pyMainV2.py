# -*- coding: utf-8 -*-
# !/usr/bin/python3
import os

from dotenv import load_dotenv
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from pyBibliotecaV2 import checkFolder, StatusServidor, printTimeData, countdown, printDebug, unzipBase, parseHTMLFile, \
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
            bsHtml = parseHTMLFile(folderZip)

            pathDeleteZipUploads = f"{os.getcwd()}/{DIRNOVOS}{os.path.splitext(fileName)[0]}"
            pathDeleteExtracao = f"{os.getcwd()}/{DIREXTRACAO}{os.path.splitext(fileName)[0]}"

            if bsHtml is not None and bsHtml != "" and Unidade is not None:

                NomeUnidade = find_unidade_postgres(Unidade)

                fileDados = readHeader(bsHtml)

                # DADOS
                ipaddresses = readipAddresses(bsHtml)
                if ipaddresses is not None:
                    fileDados['ipAddresses'] = ipaddresses

                groupsinfo = readGroup(bsHtml)
                if groupsinfo is not None:
                    fileDados['groupsInfo'] = groupsinfo

                bookinfo = readBook(bsHtml)
                if bookinfo is not None:
                    fileDados['addressBookInfo'] = bookinfo

                deviveinfo = readDevice(bsHtml)
                if deviveinfo is not None:
                    fileDados['deviceinfo'] = deviveinfo

                webinfo = readWebInfo(bsHtml)
                if webinfo is not None:
                    fileDados['deviceinfo'] = webinfo

                profileinfo = readProfileInfo(bsHtml)
                if profileinfo is not None:
                    fileDados['profile'] = profileinfo

                # PRTT
                # messages = readMessageLogs(bsHtml)
                # if messages is not None:
                #     fileDados['msgLogs'] = messages
                #
                # calls = readCallLogs(bsHtml)
                # if calls is not None:
                #     fileDados['callLogs'] = calls

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


def readHeader(bsHtml):
    header = {}

    service = bsHtml.find(text="Service")
    internal_ticket_number = bsHtml.find(text="Internal Ticket Number")
    account_identifier = bsHtml.find(text="Account Identifier")
    account_type = bsHtml.find(text="Account Type")
    generated = bsHtml.find(text="Generated")
    date_range = bsHtml.find(text="Date Range")
    ncmec_reports_definition = bsHtml.find(text="Ncmec Reports Definition")
    ncmec_cybertip_numbers = bsHtml.find(text="NCMEC CyberTip Numbers")
    emails_definition = bsHtml.find(text="Emails Definition")
    registered_email_addresses = bsHtml.find(text="Registered Email Addresses")

    if service:
        header['Service'] = service.find_next().text.strip()
    else:
        header['Service'] = None

    if internal_ticket_number:
        header['InternalTicketNumber'] = somentenumero(internal_ticket_number.find_next().text)
    else:
        header['InternalTicketNumber'] = None

    if account_identifier:
        header['AccountIdentifier'] = somentenumero(account_identifier.find_next().text)
    else:
        header['AccountIdentifier'] = None

    if account_type:
        header['AccountType'] = account_type.find_next().text.strip()
    else:
        header['AccountType'] = None

    if generated:
        header['Generated'] = generated.find_next().text.strip()
    else:
        header['Generated'] = None

    if date_range:
        header['DateRange'] = date_range.find_next().text
    else:
        header['DateRange'] = None

    if ncmec_reports_definition:
        header['NcmecReportsDefinition'] = ncmec_reports_definition.find_next().text
    else:
        header['NcmecReportsDefinition'] = None

    if ncmec_cybertip_numbers:
        header['NCMECCyberTipNumbers'] = ncmec_cybertip_numbers.find_next().text.strip()
    else:
        header['NCMECCyberTipNumbers'] = None

    if emails_definition:
        header['EmailEmailsDefinition'] = emails_definition.find_next().text.strip()
    else:
        header['EmailEmailsDefinition'] = None

    if registered_email_addresses:
        header['EmailAddresses'] = registered_email_addresses.find_next().text.strip()
    else:
        header['EmailAddresses'] = None

    if len(header) > 0:
        print("\nHeader Info")
        print(f"{header}")

        return header
    else:
        return None


def readipAddresses(bsHtml):
    logsips = []

    logsip_sections = bsHtml.find_all(text='Time')

    if logsip_sections:

        for section in logsip_sections:
            logsip_data = {}

            Time = section.find_next().find_next().text.strip()
            IPAddress = section.find_next(text='IP Address').find_next().text.strip()

            if Time is not None and IPAddress is not None:
                logsip_data['Time'] = Time
                logsip_data['IPAddress'] = IPAddress

                logsips.append(logsip_data)

    if len(logsips) > 0:
        print("\nIp Addresses")
        print(f"{logsips}")

        return logsips
    else:
        return None


def readGroup(bsHtml):
    allRegistros = []

    # ownedRegistros = []
    # participatingRegistros = []

    picture_section = bsHtml.find_all(text='Picture')

    if picture_section:

        for section in picture_section:
            picture_data = {}

            # LinkedMediaFile = section.find_next(text='Linked Media File:').find_next().text.strip()
            Thumbnail = section.find_next(text='Thumbnail').find_next().text.strip()
            ID = section.find_next(text='ID').find_next().text.strip()
            Creation = section.find_next(text='Creation').find_next().text.strip()
            # # Description = section.find_next(text='Description').find_next().text.strip()
            Size = section.find_next(text='Size').find_next().text.strip()
            Subject = section.find_next(text='Size').find_next().text.strip()

            #picture_data['LinkedMediaFile'] = LinkedMediaFile
            picture_data['Thumbnail'] = Thumbnail
            picture_data['ID'] = ID
            picture_data['Creation'] = Creation
            # picture_data['Description'] = Description
            picture_data['Size'] = Size
            picture_data['Subject'] = Subject

            allRegistros.append(picture_data)

            # if GroupOwned and picture_data not in ownedRegistros:
            #     ownedRegistros.append(picture_data)
            #
            # if GroupParticipating and picture_data not in participatingRegistros:
            #     participatingRegistros.append(picture_data)

    # allRegistros['ownedGroups'] = ownedRegistros
    # allRegistros['ParticipatingGroups'] = participatingRegistros

    if len(allRegistros) > 0:
        print("\nGroup Info")
        print(f"{allRegistros}")

        return allRegistros
    else:
        return None


def readBook(bsHtml):
    allRegistros = []

    data = {}
    sectionsSymmetric = bsHtml.find_all(text='Symmetric contacts')

    if sectionsSymmetric:
        for section in sectionsSymmetric:
            currentSymmetric = section.find_next()

            if currentSymmetric:
                # Obter o texto dentro da tag <div>
                phone_text = currentSymmetric.get_text(separator='\n')
                # Dividir o texto em uma lista usando quebras de linha
                phone_list = phone_text.split('\n')

                data['Symmetric'] = phone_list[1:]

    sectionsAsymmetric = bsHtml.find_all(text='Asymmetric contacts')
    if sectionsAsymmetric:
        for section in sectionsAsymmetric:
            currentAsymmetric = section.find_next()

            if currentAsymmetric:
                # Obter o texto dentro da tag <div>
                phone_text = currentAsymmetric.get_text(separator='\n')
                # Dividir o texto em uma lista usando quebras de linha
                phone_list = phone_text.split('\n')

                data['Asymmetric'] = phone_list[1:]

    allRegistros.append(data)

    if len(data['Symmetric']) > 0 and len(data['Asymmetric']) > 0:
        print("\nBook Info")
        print(f"{allRegistros}")

        return allRegistros
    else:
        return None


def readWebInfo(bsHtml):
    Web = {}

    WebInfo = bsHtml.find(text="Web Info")
    Availability = bsHtml.find(text="Availability")
    OnlineSince = bsHtml.find(text="Online Since")
    Platform = bsHtml.find(text="Platform")
    Version = bsHtml.find(text="Version")

    if WebInfo:
        Web['WebInfo'] = WebInfo.find_next().text.strip()
    else:
        Web['WebInfo'] = None

    if Availability:
        Web['Availability'] = Availability.find_next().text.strip()
    else:
        Web['Availability'] = None

    if OnlineSince:
        Web['OnlineSince'] = OnlineSince.find_next().text.strip()
    else:
        Web['OnlineSince'] = None

    if Platform:
        Web['Platform'] = Platform.find_next().text.strip()
    else:
        Web['Platform'] = None

    if Version:
        Web['Version'] = Version.find_next().text.strip()
    else:
        Web['Version'] = None

    if len(Web) > 0:
        print("\nWeb Info")
        print(f"{Web}")

        return Web
    else:
        return None


def readDevice(bsHtml):
    Device = {}

    DeviceId = bsHtml.find(text="Device Id")
    AppVersion = bsHtml.find(text="App Version")
    OSVersion = bsHtml.find(text="OS Version")
    OSBuildNumber = bsHtml.find(text="OS Build Number")
    DeviceManufacturer = bsHtml.find(text="Device Manufacturer")
    DeviceModel = bsHtml.find(text="Device Model")

    if DeviceId:
        Device['DeviceId'] = DeviceId.find_next().text.strip()
    else:
        Device['DeviceId'] = None

    if AppVersion:
        Device['AppVersion'] = AppVersion.find_next().text.strip()
    else:
        Device['AppVersion'] = None

    if OSVersion:
        Device['OSVersion'] = OSVersion.find_next().text.strip()
    else:
        Device['OSVersion'] = None

    if OSBuildNumber:
        Device['OSBuildNumber'] = OSBuildNumber.find_next().text.strip()
    else:
        Device['OSBuildNumber'] = None

    if DeviceManufacturer:
        Device['DeviceManufacturer'] = DeviceManufacturer.find_next().text.strip()
    else:
        Device['DeviceManufacturer'] = None

    if DeviceModel:
        Device['DeviceModel'] = DeviceModel.find_next().text.strip()
    else:
        Device['DeviceModel'] = None

    if len(Device) > 0:
        print("\nDevice Info")
        print(f"{Device}")

        return Device
    else:
        return None


def readProfileInfo(bsHtml):
    Profile = {}

    profile_sections = bsHtml.find(text='Profile Picture')

    if profile_sections:
        Profile['LinkedMediaFile'] = profile_sections.find_next(text='Linked Media File:').find_next()
    else:
        Profile['LinkedMediaFile'] = None

    if len(Profile) > 0:
        print("\nProfile Picture")
        print(f"{Profile}")

        return Profile
    else:
        return None


def readMessageLogs(bsHtml):
    print("\nMessage Log")
    message_blocks = bsHtml.find_all(text="Timestamp")

    if message_blocks:
        # Iterar sobre cada bloco e extrair as informações
        for block in message_blocks:
            menssage_data = {}

            timestamp = block.find_next().text.strip()
            message_id = block.find_next(text="Message Id").find_next().text.strip()
            sender = block.find_next(text="Sender").find_next().text.strip()
            #group_id = block.find_next(text="Group Id").find_next()
            recipients = block.find_next(text="Recipients").find_next().text.strip()
            sender_ip = block.find_next(text="Sender Ip").find_next().text.strip()
            sender_port = block.find_next(text="Sender Port").find_next().text.strip()
            sender_device = block.find_next(text="Sender Device").find_next().text.strip()
            msg_type = block.find_next(text="Type").find_next().text.strip()
            message_style = block.find_next(text="Message Style").find_next().text.strip()
            message_size = block.find_next(text="Message Size").find_next().text.strip()

            #Imprimir as informações extraídas
            print(f"Timestamp: {timestamp}")
            print(f"Message Id: {message_id}")
            print(f"Sender: {sender}")
            print(f"Recipients: {recipients}")
            print(f"Sender Ip: {sender_ip}")
            print(f"Sender Port: {sender_port}")
            print(f"Sender Device: {sender_device}")
            print(f"Type: {msg_type}")
            print(f"Message Style: {message_style}")
            print(f"Message Size: {message_size}")
            print("\n---\n")


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

            call_data['callID'] = parent.find_next(text='Call Id').find_next().text.strip()
            call_data['callCreator'] = parent.find_next(text='Call Creator').find_next().text.strip()

            events = []

            # Percorrer os eventos dentro do bloco de chamada
            current = parent.find_next(text='Events').find_next()

            while current and current.text.strip() not in ['Call Id', 'Events']:

                if 'Type' in current.text.strip():
                    event_data = {}
                    event_data['type'] = current.find_next().text.strip()
                    event_data['timestamp'] = current.find_next(text='Timestamp').find_next().text.strip()
                    event_data['solicitante'] = current.find_next(text='From').find_next().text.strip()
                    event_data['atendente'] = current.find_next(text='To').find_next().text.strip()
                    event_data['solIP'] = current.find_next(text='From Ip').find_next().text.strip()
                    event_data['solPort'] = current.find_next(text='From Port').find_next().text.strip()
                    event_data['mediaType'] = current.find_next(
                        text='Media Type').find_next().text.strip() if current.find_next(
                        text='Media Type') else 'N/A'
                    events.append(event_data)
                    current = current.find_next(text='Type')
                else:
                    current = current.find_next()

            call_data['Events'] = events

            if call_data and call_data not in calls:
                calls.append(call_data)

    if len(calls) > 0:
        print(f"{calls}")
        return calls
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
