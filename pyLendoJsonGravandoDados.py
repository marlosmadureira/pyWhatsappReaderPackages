import os, time

from pyBibliotecaLendoJsonGravandoDados import conectBD, checkFolder, somentenumero, get_files_in_dir, openJson, delete_log
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

load_dotenv()

DIRNOVOS = os.getenv("DIRNOVOS")
DIRLIDOS = os.getenv("DIRLIDOS")
DIRERROS = os.getenv("DIRERROS")
DIREXTRACAO = os.getenv("DIREXTRACAO")
DIRLOG = os.getenv("DIRLOG")

executaSql = False
PrintSql = True

def sendDataPostgres(Dados, type, Out):
    indice = 0

    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

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

        if Dados.get('Service'):
            Service = Dados['Service']
        else:
            Service = None

        if Dados.get('EmailAddresses'):
            EmailAddresses = Dados['EmailAddresses']
        else:
            EmailAddresses = None

        if AccountIdentifier is not None and Unidade is not None:

            sqlTratamento = f"SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL;"

            if PrintSql:
                indice += 1
                print(f"1 {indice} - {sqlTratamento}")

            try:
                db.execute(sqlTratamento)
                queryTratamento = db.fetchall()
            except:
                pass

            if queryTratamento is not None:
                for dado in queryTratamento:
                    apli_id = dado[0]
                    linh_id = dado[1]
                    conta_id = somentenumero(dado[2])

                    sqlUpdate = f"UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = %s WHERE conta_zap IS NULL AND apli_id = %s AND linh_id = %s"

                    if executaSql:
                        try:
                            db.execute(sqlUpdate, (conta_id, apli_id, linh_id))

                            if PrintSql:
                                indice += 1
                                print(f"2 {indice} - {db.query}")

                            con.commit()
                        except:
                            db.execute("rollback")
                            pass

            sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.unid_id = {Unidade} AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"

            if PrintSql:
                indice += 1
                print(f"3 {indice} - {sqllinh_id}")

            queryLinId = None

            try:
                db.execute(sqllinh_id)
                queryLinId = db.fetchone()
            except:
                pass


            if queryLinId is not None and queryLinId[0] > 0:

                linh_id = queryLinId[0]

                sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}'"

                if PrintSql:
                    indice += 1
                    print(f"4 {indice} - {sqlexistente}")

                try:
                    db.execute(sqlexistente)
                    queryExiste = db.fetchone()
                except:
                    pass

                if queryExiste is None:

                    if 'DADOS' in type:
                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1, '{EmailAddresses}') RETURNING ar_id;"

                        if PrintSql:
                            indice += 1
                            print(f"5 {indice} - {sqlInsert}")

                    if 'PRTT' in type:
                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 0, 1, '{EmailAddresses}') RETURNING ar_id;"

                        if PrintSql:
                            indice += 1
                            print(f"6 {indice} - {sqlInsert}")

                    ar_id = None

                    if executaSql:
                        try:
                            db.execute(sqlInsert)
                            con.commit()
                            result = db.fetchone()
                            if result is not None and result[0] is not None:
                                ar_id = result[0]
                        except:
                            db.execute("rollback")
                            pass

                    if ar_id is not None:

                        if 'DADOS' in type:

                            if Dados['Dados'].get('EmailAddresses'):
                                EmailAddresses = Dados['Dados'].get('EmailAddresses')

                                sqlUpdate = f"UPDATE leitores.tb_whatszap_arquivo SET ar_email_addresses = %s WHERE ar_id = %s"

                                if executaSql:
                                    try:
                                        db.execute(sqlUpdate, (EmailAddresses, ar_id))

                                        if PrintSql:
                                            indice += 1
                                            print(f"7 {indice} - {db.query}")

                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

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

                                        if dadoIPAddress is not None and dadoTime is not None:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s)";

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    dadoIPAddress, dadoTime, AccountIdentifier, ar_id, linh_id))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"8 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

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

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert,
                                                   (dadoServiceStart, dadoDeviceType, dadoAppVersion,
                                                    dadoDeviceOSBuildNumber, dadoConnectionState, dadoOnlineSince,
                                                    dadoPushName, dadoLastSeen, AccountIdentifier, ar_id, linh_id,
                                                    dadoServiceStart, dadoDeviceType, dadoAppVersion,
                                                    dadoDeviceOSBuildNumber, AccountIdentifier))

                                        if PrintSql:
                                            indice += 1
                                            print(f"9 {indice} - {db.query}")

                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

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

                                if webInfo.get('Availability'):
                                    Availability = webInfo['Availability']
                                else:
                                    Availability = None

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert, (
                                        dadoVersion, dadoPlatform, dadoOnlineSince, dadoInactiveSince,
                                        AccountIdentifier, ar_id, linh_id, dadoVersion, dadoPlatform,
                                        AccountIdentifier))

                                        if PrintSql:
                                            indice += 1
                                            print(f"10 {indice} - {db.query}")

                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

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

                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        dadoTipoGroup, pathFile, dadoThumbnail, dadoID, dadoCreation,
                                                        dadoSize, dadoDescription, dadoSubject, AccountIdentifier,
                                                        ar_id,
                                                        dadoPicture, linh_id, dadoTipoGroup, dadoCreation, dadoID,
                                                        AccountIdentifier))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"11 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

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

                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        dadoTipoGroup, pathFile, dadoThumbnail, dadoID, dadoCreation,
                                                        dadoSize, dadoDescription, dadoSubject, AccountIdentifier,
                                                        ar_id,
                                                        dadoPicture, linh_id, dadoTipoGroup, dadoCreation, dadoID,
                                                        AccountIdentifier))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"12 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    if Out:
                                        print(f"{ParticipatingGroups}")

                            if Dados['Dados'].get('addressBookInfo'):
                                if Dados['Dados']['addressBookInfo'][0].get('Symmetriccontacts'):
                                    symmetricContacts = Dados['Dados']['addressBookInfo'][0]['Symmetriccontacts']
                                    if len(symmetricContacts) > 0:
                                        for contacts in symmetricContacts:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    contacts, 'S', AccountIdentifier, ar_id, linh_id, contacts, 'S',
                                                    AccountIdentifier))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"13 {indice} - {db.query}")


                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    if Out:
                                        print(f"{symmetricContacts}")

                                if Dados['Dados']['addressBookInfo'][0].get('Asymmetriccontacts'):
                                    asymmetricContacts = Dados['Dados']['addressBookInfo'][0]['Asymmetriccontacts']
                                    if len(asymmetricContacts) > 0:
                                        for contacts in asymmetricContacts:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        contacts, 'A', AccountIdentifier, ar_id, linh_id, contacts, 'A',
                                                        AccountIdentifier))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"14 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    if Out:
                                        print(f"{asymmetricContacts}")

                            # FALTA AMOSTRA
                            if Dados['Dados'].get('ncmecReportsInfo'):
                                if Dados['Dados']['ncmecReportsInfo'].get('NcmecReportsDefinition'):
                                    NcmecReportsDefinition = Dados['Dados']['NcmecReportsDefinition'][
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

                            # FALTA AMOSTRA
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

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_deviceinfo (dev_appversion, dev_osversion, dev_buildnumber, dev_manufacturer, dev_devicemodel, ar_id, linh_id, telefone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert, (
                                            AppVersion, OSVersion, OSBuildNumber, DeviceManufacturer, DeviceModel,
                                            ar_id,
                                            linh_id, AccountIdentifier, AppVersion, OSVersion, OSBuildNumber,
                                            AccountIdentifier))

                                        if PrintSql:
                                            indice += 1
                                            print(f"15 {indice} - {db.query}")

                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

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
                                        if prttSender == AccountIdentifier:
                                            TipoDirecaoMsg = "Enviou";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    prttTimestamp, prttMessageId, TipoDirecaoMsg, prttSender,
                                                    prttRecipients, prttSenderIp, prttSenderPort, prttSenderDevice,
                                                    prttMessageSize, prttType, prttMessageStyle, AccountIdentifier,
                                                    ar_id, linh_id))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                        else:
                                            TipoDirecaoMsg = "Recebeu";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    prttTimestamp, prttMessageId, TipoDirecaoMsg, prttRecipients,
                                                    prttSender, prttSenderIp, prttSenderPort, prttSenderDevice,
                                                    prttMessageSize, prttType, prttMessageStyle, AccountIdentifier,
                                                    ar_id, linh_id))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    else:
                                        if prttSender == AccountIdentifier:
                                            TipoDirecaoMsg = "Enviou";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    prttTimestamp, prttMessageId, TipoDirecaoMsg, prttSender,
                                                    prttRecipients, prttGroupId, prttSenderIp, prttSenderPort,
                                                    prttSenderDevice, prttMessageSize, prttType, prttMessageStyle,
                                                    AccountIdentifier, ar_id, linh_id))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                        else:
                                            TipoDirecaoMsg = "Recebeu";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    prttTimestamp, prttMessageId, TipoDirecaoMsg, prttRecipients,
                                                    prttSender, prttGroupId, prttSenderIp, prttSenderPort,
                                                    prttSenderDevice, prttMessageSize, prttType, prttMessageStyle,
                                                    AccountIdentifier, ar_id, linh_id))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

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

                                                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                                    if executaSql:
                                                        try:
                                                            db.execute(sqlInsert, (
                                                            prttcallID, prttcallCreator, prttEtype, prttEtimestamp,
                                                            prttEsolicitante, prttEatendente, prttEsolIP, prttEsolPort,
                                                            prttEmediaType, prttPhoneNumber, AccountIdentifier, ar_id,
                                                            linh_id, TipoDirecaoCall))
                                                            con.commit()
                                                        except:
                                                            db.execute("rollback")
                                                            pass

                                            else:
                                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                                                if executaSql:
                                                    try:
                                                        db.execute(sqlInsert, (
                                                        prttcallID, prttcallCreator, prttEtype, prttEtimestamp,
                                                        prttEsolicitante, prttEatendente, prttEsolIP, prttEsolPort,
                                                        prttEmediaType, prttPhoneNumber, AccountIdentifier, ar_id,
                                                        linh_id, TipoDirecaoCall))
                                                        con.commit()
                                                    except:
                                                        db.execute("rollback")
                                                        pass

                                if Out:
                                    print(f"{callLogs}")

                else:
                    print(f"\nARQUIVO EXISTNTE {FileName}")

            else:

                if type == "GDADOS":
                    sqlGrupo = f"SELECT tbobje_whatsappgrupos.grupo_id, tbobje_intercepta.linh_id, tbobje_intercepta.obje_id FROM interceptacao.tbobje_whatsappgrupos, interceptacao.tbobje_intercepta WHERE tbobje_intercepta.obje_id = tbobje_whatsappgrupos.obje_id AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.unid_id = {Unidade} AND tbobje_whatsappgrupos.grupo_id ILIKE '%{AccountIdentifier}%'"

                    if PrintSql:
                        indice += 1
                        print(f"16 {indice} - {sqlGrupo}")

                    try:
                        db.execute(sqlGrupo)
                        queryGrupo = db.fetchone()
                    except:
                        pass

                    if queryGrupo is not None and queryGrupo[0] > 0:
                        queryArId = None

                        linh_id = queryGrupo[1]

                        sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 2 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}'"

                        if PrintSql:
                            indice += 1
                            print(f"17 {indice} - {sqlexistente}")

                        try:
                            db.execute(sqlexistente)
                            queryExiste = db.fetchone()
                        except:
                            pass

                        if queryExiste is None and queryExiste[0] > 0:

                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 2, 1, '{EmailAddresses}') RETURNING ar_id;"

                            if PrintSql:
                                indice += 1
                                print(f"18 {indice} - {sqlInsert}")

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
                                sqlIdentificador = f"SELECT tbmembros_whats.identificador FROM whatsapp.tbmembros_whats, whatsapp.tbgrupowhatsapp, linha_imei.tbaplicativo_linhafone, linha_imei.tblinhafone WHERE  tbmembros_whats.grupo_id = tbgrupowhatsapp.grupo_id AND tbmembros_whats.identificador = tbaplicativo_linhafone.identificador AND tblinhafone.linh_id = tbaplicativo_linhafone.linh_id AND tblinhafone.unid_id = {Unidade} AND tbmembros_whats.grupo_id ILIKE '%{AccountIdentifier}%'"

                                if PrintSql:
                                    indice += 1
                                    print(f"19 {indice} - {sqlIdentificador}")

                                try:
                                    db.execute(sqlIdentificador)
                                    queryIdentificador = db.fetchone()
                                except:
                                    pass

                                if queryIdentificador is None and queryIdentificador[0] > 0:
                                    identificador = queryIdentificador[0]

                                    # Processar GroupParticipants
                                    if Dados['GDados']['groupsInfo'].get('GroupParticipants'):
                                        for registro in Dados['GDados']['groupsInfo']['GroupParticipants']:
                                            sqlInsert = f"INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES (%s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (AccountIdentifier.strip(), somentenumero(registro), 'N', 'A', identificador))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"20 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    # Processar GroupAdministrators
                                    if Dados['GDados']['groupsInfo'].get('GroupAdministrators'):
                                        for registro in Dados['GDados']['groupsInfo']['GroupAdministrators']:
                                            sqlInsert = f"INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES (%s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    AccountIdentifier.strip(), somentenumero(registro), 'S', 'A',
                                                    identificador))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"21 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                    # Processar Participants
                                    if Dados['GDados']['groupsInfo'].get('Participants'):
                                        for registro in Dados['GDados']['groupsInfo']['Participants']:
                                            sqlInsert = f"INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES (%s, %s, %s, %s, %s);"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    AccountIdentifier.strip(), somentenumero(registro), 'N', 'A',
                                                    identificador))

                                                    if PrintSql:
                                                        indice += 1
                                                        print(f"22 {indice} - {db.query}")

                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass
                else:

                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES ({linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 0, 1, '{EmailAddresses}') RETURNING ar_id;"

                    if PrintSql:
                        indice += 1
                        print(f"23 {indice} - {sqlInsert}")

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


        else:
            print(f"\nNÃO LOCALIZADO A CONTA {AccountIdentifier}\n")

    db.close()
    con.close()

if __name__ == '__main__':
    checkFolder(DIRNOVOS)
    checkFolder(DIRLIDOS)
    checkFolder(DIRERROS)
    checkFolder(DIREXTRACAO)
    checkFolder(DIRLOG)

    previous_files = get_files_in_dir(DIRNOVOS)

    dttmpstatus = ""
    print(f"\nMicroServiço = Escuta Pasta Whatsapp ZipUploads\n")

    while True:
        time.sleep(3)
        current_files = get_files_in_dir(DIRNOVOS)
        added_files = current_files - previous_files
        removed_files = previous_files - current_files

        try:
            if added_files:
                for file in added_files:
                    nameFile = f"{DIRNOVOS}{file}"
                    Dados = openJson(nameFile)
                    parts = nameFile.split("_")

                    dataType = parts[1]

                    sendDataPostgres(Dados, dataType, False)

                    delete_log(nameFile)

            if removed_files:
                print(f'\nArquivos removidos: {removed_files}')
        except Exception as inst:
            pass

        previous_files = current_files