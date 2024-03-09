import os


from pyBiblioteca import conectBD, grava_log, somentenumero
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

executaSql = False


def sendDataPostgres(Dados, type, DebugMode, Out, fileName):
    Out = True

    grava_log(Dados, f'{type}_{fileName}.json')

    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        logSql = False

        if Dados.get('FileName'):
            FileName = Dados['FileName']
        else:
            FileName = None

        if Dados.get('Unidade'):
            Unidade = Dados['Unidade']
        else:
            Unidade = None

        if Dados.get('InternalTicketNumber'):
            InternalTicketNumber = Dados['InternalTicketNumber']
        else:
            InternalTicketNumber = None

        if Dados.get('AccountIdentifier'):
            AccountIdentifier = somentenumero(Dados['AccountIdentifier'])
        else:
            AccountIdentifier = None

        if Dados.get('AccountType'):
            AccountType = Dados['AccountType']
        else:
            AccountType = None

        if Dados.get('Generated'):
            Generated = Dados['Generated']
        else:
            Generated = None

        if Dados.get('DateRange'):
            DateRange = Dados['DateRange']
        else:
            DateRange = None

        if Dados.get('EmailAddresses'):
            EmailAddresses = Dados['EmailAddresses']
        else:
            EmailAddresses = None

        if AccountIdentifier is not None and Unidade is not None:

            sqlTratamento = f"SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL;"

            if executaSql:
                try:
                    db.execute(sqlTratamento)
                    queryTratamento = db.fetchone()
                except:
                    pass

            if queryTratamento is not None and queryTratamento[0] > 0:
                apli_id = queryTratamento[0]
                linh_id = queryTratamento[1]
                conta_id = somentenumero(queryTratamento[2])

                sqlUpdate = f"UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '%s' WHERE conta_zap IS NULL AND apli_id = %s AND linh_id = %s"

                if executaSql:
                    try:
                        db.execute(sqlUpdate, (conta_id, apli_id, linh_id))
                        con.commit()
                    except:
                        db.execute("rollback")
                        pass

            sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"

            if executaSql:
                try:
                    db.execute(sqllinh_id)
                    queryLinId = db.fetchone()
                except:
                    pass

            if queryLinId is not None and queryLinId[0] > 0:

                linh_id = queryLinId[0]

                sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}'"

                if executaSql:
                    try:
                        db.execute(sqlexistente)
                        queryExiste = db.fetchone()
                    except:
                        pass

                if queryExiste is None:

                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) SELECT {linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1, '{EmailAddresses}' RETURNING ar_id;"

                    if executaSql:
                        try:
                            db.execute(sqlInsert)
                            con.commit()
                            result = db.fetchone()
                            if result is not None and result[0] is not None:
                                ar_id = result[0]
                            else:
                                ar_id = None
                        except:
                            db.execute("rollback")
                            pass

                    if ar_id is not None:
                        if 'DADOS' in type:

                            if Dados['Dados'].get('EmailAddresses'):
                                EmailAddresses = Dados['Dados'].get('EmailAddresses')

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{EmailAddresses}")

                            if Dados['Dados'].get('ipAddresses'):
                                ipAddresses = Dados['Dados']['ipAddresses']
                                if len(ipAddresses) > 0:
                                    for registro in ipAddresses:
                                        if registro.get('IPAddress'):
                                            dadoIPAddress = registro['IPAddress']
                                        else:
                                            dadoIPAddress = None

                                        if registro.get('Time'):
                                            dadoTime = registro['Time'].replace("UTC", "")
                                        else:
                                            dadoTime = None

                                        print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{ipAddresses}")

                            if Dados['Dados'].get('connectionInfo'):
                                connectionInfo = Dados['Dados']['connectionInfo']

                                if connectionInfo.get('Servicestart'):
                                    dadoServiceStart = connectionInfo['Servicestart']
                                else:
                                    dadoServiceStart = None

                                if connectionInfo.get('DeviceType'):
                                    dadoDeviceType = connectionInfo['DeviceType']
                                else:
                                    dadoDeviceType = None

                                if connectionInfo.get('AppVersion'):
                                    dadoAppVersion = connectionInfo['AppVersion']
                                else:
                                    dadoAppVersion = None

                                if connectionInfo.get('DeviceOSBuildNumber'):
                                    dadoDeviceOSBuildNumber = connectionInfo['DeviceOSBuildNumber']
                                else:
                                    dadoDeviceOSBuildNumber = None

                                if connectionInfo.get('ConnectionState'):
                                    dadoConnectionState = connectionInfo['ConnectionState']
                                else:
                                    dadoConnectionState = None

                                if connectionInfo.get('OnlineSince'):
                                    dadoOnlineSince = connectionInfo['OnlineSince']
                                else:
                                    dadoOnlineSince = None

                                if connectionInfo.get('PushName'):
                                    dadoPushName = connectionInfo['PushName']
                                else:
                                    dadoPushName = None

                                if connectionInfo.get('Lastseen'):
                                    dadoLastSeen = connectionInfo['Lastseen']
                                else:
                                    dadoLastSeen = None

                                if connectionInfo.get('LastIP'):
                                    dadoLastIP = connectionInfo['LastIP']
                                else:
                                    dadoLastIP = None

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{connectionInfo}")

                            if Dados['Dados'].get('webInfo'):
                                webInfo = Dados['Dados']['webInfo']
                                if webInfo.get('Version'):
                                    dadoVersion = webInfo['Version']
                                else:
                                    dadoVersion = None

                                if webInfo.get('Platform'):
                                    dadoPlatform = webInfo['Platform']
                                else:
                                    dadoPlatform = None

                                if webInfo.get('OnlineSince'):
                                    dadoOnlineSince = webInfo['OnlineSince']
                                else:
                                    dadoOnlineSince = None

                                if webInfo.get('InactiveSince'):
                                    dadoInactiveSince = webInfo['InactiveSince']
                                else:
                                    dadoInactiveSince = None

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{webInfo}")

                            if Dados['Dados'].get('groupsInfo'):
                                if Dados['Dados']['groupsInfo'].get('ownedGroups'):
                                    ownedGroups = Dados['Dados']['groupsInfo']['ownedGroups']
                                    if len(ownedGroups) > 0:
                                        dadoTipoGroup = 'Owned'
                                        pathFile = None
                                        for registro in ownedGroups:
                                            if registro.get('Picture'):
                                                dadoPicture = registro['Picture']
                                            else:
                                                dadoPicture = None

                                            if registro.get('Thumbnail'):
                                                dadoThumbnail = registro['Thumbnail']
                                            else:
                                                dadoThumbnail = None

                                            if registro.get('ID'):
                                                dadoID = registro['ID']
                                            else:
                                                dadoID = None

                                            if registro.get('Creation'):
                                                dadoCreation = registro['Creation']
                                            else:
                                                dadoCreation = None

                                            if registro.get('Size'):
                                                dadoSize = registro['Size']
                                            else:
                                                dadoSize = None

                                            if registro.get('Description'):
                                                dadoDescription = registro['Description']
                                            else:
                                                dadoDescription = None

                                            if registro.get('Subject'):
                                                dadoSubject = registro['Subject']
                                            else:
                                                dadoSubject = None

                                            print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                    if Out:
                                        print(f"{ownedGroups}")

                                if Dados['Dados']['groupsInfo'].get('ParticipatingGroups'):
                                    ParticipatingGroups = Dados['Dados']['groupsInfo']['ParticipatingGroups']
                                    if len(ParticipatingGroups) > 0:
                                        dadoTipoGroup = 'Participating'
                                        pathFile = None
                                        for registro in ParticipatingGroups:
                                            if registro.get('Picture'):
                                                dadoPicture = registro['Picture']
                                            else:
                                                dadoPicture = None

                                            if registro.get('Thumbnail'):
                                                dadoThumbnail = registro['Thumbnail']
                                            else:
                                                dadoThumbnail = None

                                            if registro.get('ID'):
                                                dadoID = registro['ID']
                                            else:
                                                dadoID = None

                                            if registro.get('Creation'):
                                                dadoCreation = registro['Creation']
                                            else:
                                                dadoCreation = None

                                            if registro.get('Size'):
                                                dadoSize = registro['Size']
                                            else:
                                                dadoSize = None

                                            if registro.get('Description'):
                                                dadoDescription = registro['Description']
                                            else:
                                                dadoDescription = None

                                            if registro.get('Subject'):
                                                dadoSubject = registro['Subject']
                                            else:
                                                dadoSubject = None

                                            print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                    if Out:
                                        print(f"{ParticipatingGroups}")

                            if Dados['Dados'].get('addressBookInfo'):
                                if Dados['Dados']['addressBookInfo'][0].get('Symmetriccontacts'):
                                    symmetricContacts = Dados['Dados']['addressBookInfo'][0]['Symmetriccontacts']
                                    if len(symmetricContacts) > 0:
                                        for contacts in symmetricContacts:
                                            print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                    if Out:
                                        print(f"{symmetricContacts}")

                                if Dados['Dados']['addressBookInfo'][0].get('Asymmetriccontacts'):
                                    asymmetricContacts = Dados['Dados']['addressBookInfo'][0]['Asymmetriccontacts']
                                    if len(asymmetricContacts) > 0:
                                        for contacts in asymmetricContacts:
                                            print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                    if Out:
                                        print(f"{asymmetricContacts}")

                            if Dados['Dados'].get('ncmecReportsInfo'):
                                if Dados['Dados']['ncmecReportsInfo'].get('NcmecReportsDefinition'):
                                    NcmecReportsDefinition = Dados['Dados']['ncmecReportsInfo'][
                                        'NcmecReportsDefinition']
                                else:
                                    NcmecReportsDefinition = None

                                if Dados['Dados']['ncmecReportsInfo'].get('NCMECCyberTipNumbers'):
                                    NCMECCyberTipNumbers = Dados['Dados']['ncmecReportsInfo']['NCMECCyberTipNumbers']
                                else:
                                    NCMECCyberTipNumbers = None

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{Dados['Dados'].get('ncmecReportsInfo')}")

                            if Dados['Dados'].get('smallmediumbusinessinfo'):
                                smallMediumBusiness = Dados['Dados']['smallmediumbusinessinfo']

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{smallMediumBusiness}")

                            if Dados['Dados'].get('deviceinfo'):
                                if Dados['Dados']['deviceinfo'].get('AppVersion'):
                                    AppVersion = Dados['Dados']['deviceinfo']['AppVersion']
                                else:
                                    AppVersion = None

                                if Dados['Dados']['deviceinfo'].get('OSVersion'):
                                    OSVersion = Dados['Dados']['deviceinfo']['OSVersion']
                                else:
                                    OSVersion = None

                                if Dados['Dados']['deviceinfo'].get('OSBuildNumber'):
                                    OSBuildNumber = Dados['Dados']['deviceinfo']['OSBuildNumber']
                                else:
                                    OSBuildNumber = None

                                if Dados['Dados']['deviceinfo'].get('DeviceManufacturer'):
                                    DeviceManufacturer = Dados['Dados']['deviceinfo']['DeviceManufacturer']
                                else:
                                    DeviceManufacturer = None

                                if Dados['Dados']['deviceinfo'].get('DeviceModel'):
                                    DeviceModel = Dados['Dados']['deviceinfo']['DeviceModel']
                                else:
                                    DeviceModel = None

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{Dados['Dados'].get('deviceinfo')}")

                        if 'PRTT' in type:
                            if Dados['Prtt'].get('msgLogs'):
                                msgLogs = Dados['Prtt']['msgLogs']

                                for registro in msgLogs:
                                    if registro.get('Timestamp'):
                                        prttTimestamp = registro['Timestamp'].replace("UTC", "")
                                    else:
                                        prttTimestamp = None

                                    if registro.get('MessageId'):
                                        prttMessageId = registro['MessageId']
                                    else:
                                        prttMessageId = None

                                    if registro.get('Sender'):
                                        prttSender = registro['Sender']
                                    else:
                                        prttSender = None

                                    if registro.get('Recipients'):
                                        prttRecipients = registro['Recipients']
                                    else:
                                        prttRecipients = None

                                    if registro.get('GroupId'):
                                        prttGroupId = registro['GroupId']
                                    else:
                                        prttGroupId = None

                                    if registro.get('SenderIp'):
                                        prttSenderIp = registro['SenderIp']
                                    else:
                                        prttSenderIp = None

                                    if registro.get('SenderPort'):
                                        prttSenderPort = registro['SenderPort']
                                    else:
                                        prttSenderPort = None

                                    if registro.get('SenderDevice'):
                                        prttSenderDevice = registro['SenderDevice']
                                    else:
                                        prttSenderDevice = None

                                    if registro.get('Type'):
                                        prttType = registro['Type']
                                    else:
                                        prttType = None

                                    if registro.get('MessageStyle'):
                                        prttMessageStyle = registro['MessageStyle']
                                    else:
                                        prttMessageStyle = None

                                    if registro.get('MessageSize'):
                                        prttMessageSize = registro['MessageSize']
                                    else:
                                        prttMessageSize = None

                                    if prttGroupId == None:
                                        print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")
                                    else:
                                        print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{msgLogs}")

                            if Dados['Prtt'].get('callLogs'):
                                callLogs = Dados['Prtt']['callLogs']

                                for registro in callLogs:
                                    if registro.get('CallId'):
                                        prttcallID = registro['CallId']
                                    else:
                                        prttcallID = None

                                    if registro.get('CallCreator'):
                                        prttcallCreator = registro['CallCreator']
                                    else:
                                        prttcallCreator = None

                                    if registro.get(
                                            'Events') and prttcallCreator is not None and prttcallID is not None:
                                        eventos = registro['Events']
                                        for evento in eventos:
                                            if evento.get('Type'):
                                                prttEtype = evento['Type']
                                            else:
                                                prttEtype = None

                                            if evento.get('Timestamp'):
                                                prttEtimestamp = evento['Timestamp'].replace("UTC", "")
                                            else:
                                                prttEtimestamp = None

                                            if evento.get('From'):
                                                prttEsolicitante = evento['From']
                                            else:
                                                prttEsolicitante = None

                                            if evento.get('To'):
                                                prttEatendente = evento['To']
                                            else:
                                                prttEatendente = None

                                            if evento.get('FromIp'):
                                                prttEsolIP = evento['FromIp']
                                            else:
                                                prttEsolIP = None

                                            if evento.get('FromPort'):
                                                prttEsolPort = evento['FromPort']
                                            else:
                                                prttEsolPort = None

                                            if evento.get('MediaType'):
                                                prttEmediaType = evento['MediaType']
                                            else:
                                                prttEmediaType = None

                                            if prttcallCreator == AccountIdentifier:
                                                TipoDirecaoCall = "EFETUOU";
                                            else:
                                                TipoDirecaoCall = "RECEBEU";

                                            if evento.get('PhoneNumber'):
                                                prttPhoneNumber = evento['PhoneNumber']
                                            else:
                                                prttPhoneNumber = None

                                            if evento.get('Participants'):
                                                Participants = evento['Participants']
                                                for eventoParticipante in Participants:
                                                    if eventoParticipante.get('PhoneNumber'):
                                                        prttPhoneNumber = eventoParticipante['PhoneNumber']
                                                    else:
                                                        prttPhoneNumber = None

                                                    print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")
                                            else:
                                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                if Out:
                                    print(f"{callLogs}")

                else:
                    print(f"\nARQUIVO EXISTNTE {FileName}")
            else:
                print(f"\nLINHA NÃO LOCALIZADA OU INTERCEPTADA {AccountIdentifier}\n")
        else:
            print(f"\nNÃO LOCALIZADO A CONTA {AccountIdentifier}\n")

    db.close()
    con.close()
