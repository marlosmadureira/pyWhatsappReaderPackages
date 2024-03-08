import json
import os
import re

import requests

from pyBiblioteca import conectBD, somentenumero, grava_log
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

DebugMode = False


def setDateObjetoProrrogue(AccountIdentifier, Unidade, fileName):
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        contaZap = somentenumero(AccountIdentifier)

        sqlobje_id = f"SELECT tbobje_intercepta.obje_id, tbobje_intercepta.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbaplicativo_linhafone.conta_zap = '{contaZap}' AND tbobje_intercepta.unid_id = {Unidade} AND tbobje_intercepta.obje_dtinicio IS NULL "

        if DebugMode:
            print(f"\nCONSULTA {sqlobje_id}")

        db.execute(sqlobje_id)
        query = db.fetchone()

        if query is not None:
            obje_id = query[0]
            linh_id = query[1]

            # dataInicio = datetime.now()
            # dataFinal = dataInicio + timedelta(days=15)
            # dataProrrogacao = dataFinal - timedelta(days=2)
            #
            # sqlUpdate = f"UPDATE interceptacao.tbobje_intercepta SET obje_dtinicio = %s, obje_dtprorr = %s, obje_dtfim = %s WHERE opra_id = %s AND obje_id = %s; AND (interceptado = %s OR interceptado = %s)"
            #
            # try:
            #     db.execute(sqlUpdate, (dataInicio, dataProrrogacao, dataFinal, 28, obje_id, 'I', 'P'))
            #     print(f"\nSQL {db.query}")
            #     con.commit()
            # except:
            #     print(f"\nError SQL {db.query}")
            #     db.execute("rollback")

            sqlNumOficio = f"SELECT tbnumerador.nume_nro, tbnumerador.nume_ano FROM interceptacao.tbobje_intercepta, interceptacao.tboficio, interceptacao.tbnumerador where tbobje_intercepta.ofic_id = tboficio.ofic_id AND tbnumerador.nume_id = tboficio.nume_id AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.obje_id = {obje_id} AND tbobje_intercepta.unid_id = {Unidade} AND tbobje_intercepta.linh_id = {linh_id} "
            db.execute(sqlNumOficio)
            queryOf = db.fetchone()

            if queryOf is not None:
                nume_nro = queryOf[0]
                nume_ano = queryOf[1]

                print(f"\nOFICIO = {nume_nro}/{nume_ano}")

        else:
            arquivo = "SQL_NULL.txt"
            content = f"{fileName} {sqlobje_id}"
            grava_log(content, arquivo)

    db.close()
    con.close()


def sendDataJsonServer(Dados, type):
    payload = {'token': APITOKEN, 'action': 'sendWPData', 'type': type, 'jsonData': json.dumps(Dados)}
    try:
        r = requests.post(APILINK, data=payload)

        if r.status_code == 200 and r.text != "" and r.text is not None:
            Jsondata = json.loads(r.text)

            return Jsondata
    except requests.exceptions.ConnectionError:
        print('build http connection failed')
    except Exception as inst:
        errorData = "{Location: sendDataJsonServer, error: " + str(inst) + ", type: " + type + "}"
        # sendSlackMSG(errorData)


def sendDataDumpPostgres(Dados, type):
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
            AccountIdentifier = re.sub('[^0-9]', '', Dados['AccountIdentifier'])
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

        if AccountIdentifier is not None:
            sqlTratamento = f"SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE apli_id = 1 AND conta_zap IS NULL"
            db.execute(sqlTratamento)
            queryTratamento = db.fetchone()

            if logSql:
                print('Log Tratamento ', sqlTratamento)

            if queryTratamento is not None and queryTratamento[0] > 0:
                apli_id = queryTratamento[0]
                linh_id = queryTratamento[1]
                conta_id = re.sub('[^0-9]', '', queryTratamento[2])

                sqlUpdate = f"UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '{conta_id}' WHERE conta_zap IS NULL AND apli_id = {apli_id} AND linh_id = {linh_id}"

                try:
                    db.execute(sqlUpdate)
                    con.commit()
                except:
                    db.execute("rollback")

                if logSql:
                    print('Log Update ', sqlUpdate)

            sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"

            try:
                db.execute(sqllinh_id)
                queryLinId = db.fetchone()
            except:
                pass

            if logSql:
                print('Log Select Linh_id ', sqllinh_id)

            if queryLinId is not None and queryLinId[0] > 0:

                linh_id = queryLinId[0]

                if type == "Dados":

                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) SELECT {linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1, '{EmailAddresses}' WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}') RETURNING ar_id;";

                    try:
                        db.execute(sqlInsert)
                        con.commit()
                        result = db.fetchone()
                        if result is not None and result[0] is not None:
                            ar_id = result[0];
                        else:
                            ar_id = None
                    except:
                        db.execute("rollback")

                    if logSql:
                        print('Log Arqruivo Dados ', sqlInsert)

                    if ar_id is not None:
                        if Dados['Dados'].get('ipAddresses'):
                            ipAddresses = Dados['Dados']['ipAddresses']
                            if len(ipAddresses) > 0:
                                for registro in ipAddresses:
                                    if registro.get('Timestamp'):
                                        dadoIPAddress = registro['dadoIPAddress']
                                    else:
                                        dadoIPAddress = None

                                    if registro.get('Timestamp'):
                                        dadoTime = registro['dadoTime'].replace("UTC", "")
                                    else:
                                        dadoTime = None

                                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) SELECT '{dadoIPAddress}', '{dadoTime}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_iptime WHERE ip_ip = '{dadoIPAddress}' AND ip_tempo = '{dadoTime}' AND telefone = '{AccountIdentifier}');";

                                    try:
                                        db.execute(sqlInsert)
                                        con.commit()
                                    except:
                                        db.execute("rollback")

                                    if logSql:
                                        print("Log IP ", sqlInsert)

                        if Dados['Dados'].get('connectionInfo'):
                            connectionInfo = Dados['Dados']['connectionInfo']

                            if connectionInfo.get('ServiceStart'):
                                dadoServiceStart = connectionInfo['ServiceStart']
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

                            if connectionInfo.get('LastSeen'):
                                dadoLastSeen = connectionInfo['LastSeen']
                            else:
                                dadoLastSeen = None

                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) SELECT '{dadoServiceStart}', '{dadoDeviceType}', '{dadoAppVersion}', '{dadoDeviceOSBuildNumber}', '{dadoConnectionState}', '{dadoOnlineSince}', '{dadoPushName}', '{dadoLastSeen}', '{AccountIdentifier}', {ar_id}, {linh_id}  WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_conexaoinfo WHERE servicestart = '{dadoServiceStart}' AND devicetype = '{dadoDeviceType}' AND appversion = '{dadoAppVersion}' AND deviceosbuildnumber = '{dadoDeviceOSBuildNumber}' AND telefone = '{AccountIdentifier}');"

                            try:
                                db.execute(sqlInsert)
                                con.commit()
                            except:
                                db.execute("rollback")

                            if logSql:
                                print("Log Conexao ", sqlInsert)

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

                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) SELECT '{dadoVersion}',{dadoPlatform}', '{dadoOnlineSince}', '{dadoInactiveSince}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_weinfo WHERE we_version = '{dadoVersion}' AND we_platform = '{dadoPlatform}' AND telefone = '{AccountIdentifier}');"

                            try:
                                db.execute(sqlInsert)
                                con.commit()
                            except:
                                db.execute("rollback")

                            if logSql:
                                print("Log Web Info ", sqlInsert)

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

                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) SELECT '{dadoTipoGroup}', '{pathFile}', '{dadoThumbnail}', '{dadoID}', '{dadoCreation}', '{dadoSize}', '{dadoDescription}', '{dadoSubject}', '{AccountIdentifier}', {ar_id}, '{dadoPicture}', {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '{dadoTipoGroup}' AND creation = '{dadoCreation}' AND id_msg = '{dadoID}' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log ownedGroups ", sqlInsert)

                            if Dados['Dados']['groupsInfo'].get('ParticipatingGroups'):
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

                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) SELECT '{dadoTipoGroup}', '{pathFile}', '{dadoThumbnail}', '{dadoID}', '{dadoCreation}', '{dadoSize}', '{dadoDescription}', '{dadoSubject}', '{AccountIdentifier}', {ar_id}, '{dadoPicture}', {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '{dadoTipoGroup}' AND creation = '{dadoCreation}' AND id_msg = '{dadoID}' AND telefone = '{AccountIdentifier}');"

                                            try:
                                                db.execute(sqlInsert)
                                                con.commit()
                                            except:
                                                db.execute("rollback")

                                            if logSql:
                                                print("Log ParticipatingGroups ", sqlInsert)

                        if Dados['Dados'].get('addressBookInfo'):
                            if Dados['Dados']['addressBookInfo'][0].get('Symmetriccontacts'):
                                symmetricContacts = Dados['Dados']['addressBookInfo'][0]['Symmetriccontacts']
                                if len(symmetricContacts) > 0:
                                    for contacts in symmetricContacts:
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) SELECT '{contacts}', 'S', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '{contacts}' AND ag_tipo = 'S' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log symmetricContacts ", sqlInsert)

                            if Dados['Dados']['addressBookInfo'][0].get('Asymmetriccontacts'):
                                asymmetricContacts = Dados['Dados']['addressBookInfo'][0]['Asymmetriccontacts']
                                if len(asymmetricContacts) > 0:
                                    for contacts in asymmetricContacts:
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) SELECT '{contacts}', 'A', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '{contacts}' AND ag_tipo = 'S' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log asymmetricContacts ", sqlInsert)

                        if Dados['Dados'].get('ncmecReportsInfo'):
                            if Dados['Dados']['ncmecReportsInfo'].get('Numbers'):
                                Numbers = Dados['Dados']['ncmecReportsInfo']['Numbers']
                            else:
                                Numbers = None

                        if Dados['Dados'].get('smallMediumBusiness'):
                            smallMediumBusiness = Dados['Dados']['smallMediumBusiness']
                        else:
                            mallMediumBusiness = None
                    else:
                        print("\nNÃO LOCALIZADO A CONTA TELEFONICA DADOS", ar_id)
                        return False

                if type == "PRTT":
                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id) SELECT '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 0, 1, {linh_id} WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}') RETURNING ar_id;"

                    try:
                        db.execute(sqlInsert)
                        con.commit()
                        result = db.fetchone()
                        if result is not None and result[0] is not None:
                            ar_id = result[0];
                        else:
                            ar_id = None
                    except:
                        db.execute("rollback")

                    if logSql:
                        print("Log Arquivo PRTT", sqlInsert)

                    if ar_id is not None:
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
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '{prttTimestamp}', '{prttMessageId}', '{TipoDirecaoMsg}', '{prttSender}', '{prttRecipients}', '{prttSenderIp}', {prttSenderPort}, '{prttSenderDevice}', {prttMessageSize}, '{prttType}', '{prttMessageStyle}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '{prttMessageId}' AND datahora = '{prttTimestamp}' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log msgLogs Individual Enviou", sqlInsert)

                                    else:
                                        TipoDirecaoMsg = "Recebeu";
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '{prttTimestamp}', '{prttMessageId}', '{TipoDirecaoMsg}', '{prttRecipients}', '{prttSender}', '{prttSenderIp}', {prttSenderPort}, '{prttSenderDevice}', {prttMessageSize}, '{prttType}', '{prttMessageStyle}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '{prttMessageId}'  AND datahora = '{prttTimestamp}' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log msgLogs Individual Recebeu", sqlInsert)
                                else:
                                    if prttSender == AccountIdentifier:
                                        TipoDirecaoMsg = "Enviou";
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '{prttTimestamp}', '{prttMessageId}', '{TipoDirecaoMsg}', '{prttSender}', '{prttRecipients}', '{prttGroupId}', '{prttSenderIp}', {prttSenderPort}, '{prttSenderDevice}', {prttMessageSize}, '{prttType}', '{prttMessageStyle}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '{prttMessageId}' AND datahora = '{prttTimestamp}' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log msgLogs Grupo Enviou", sqlInsert)

                                    else:
                                        TipoDirecaoMsg = "Recebeu";
                                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '{prttTimestamp}', '{prttMessageId}', '{TipoDirecaoMsg}', '{prttRecipients}', '{prttSender}', '{prttGroupId}', '{prttSenderIp}', {prttSenderPort}, '{prttSenderDevice}', {prttMessageSize}, '{prttType}', '{prttMessageStyle}', '{AccountIdentifier}', {ar_id}, {linh_id} WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '{prttMessageId}'  AND datahora = '{prttTimestamp}' AND telefone = '{AccountIdentifier}');"

                                        try:
                                            db.execute(sqlInsert)
                                            con.commit()
                                        except:
                                            db.execute("rollback")

                                        if logSql:
                                            print("Log msgLogs Grupo Recebeu", sqlInsert)

                        if Dados['Prtt'].get('callLogs'):
                            callLogs = Dados['Prtt']['callLogs']
                            for registro in callLogs:
                                if registro.get('callID'):
                                    prttcallID = registro['callID']
                                else:
                                    prttcallID = None

                                if registro.get('callCreator'):
                                    prttcallCreator = registro['callCreator']
                                else:
                                    prttcallCreator = None

                                if registro.get('Events') and prttcallCreator is not None and prttcallID is not None:
                                    eventos = registro['Events']
                                    for evento in eventos:
                                        if evento.get('type'):
                                            prttEtype = evento['type']
                                        else:
                                            prttEtype = None

                                        if evento.get('timestamp'):
                                            prttEtimestamp = evento['timestamp'].replace("UTC", "")
                                        else:
                                            prttEtimestamp = None

                                        if evento.get('solicitante'):
                                            prttEsolicitante = evento['solicitante']
                                        else:
                                            prttEsolicitante = None

                                        if evento.get('atendente'):
                                            prttEatendente = evento['atendente']
                                        else:
                                            prttEatendente = None

                                        if evento.get('solIP'):
                                            prttEsolIP = evento['solIP']
                                        else:
                                            prttEsolIP = None

                                        if evento.get('solPort'):
                                            prttEsolPort = evento['solPort']
                                        else:
                                            prttEsolPort = None

                                        if evento.get('mediaType'):
                                            prttEmediaType = evento['mediaType']
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

                                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) SELECT '{prttcallID}', '{prttcallCreator}', '{prttEtype}', '{prttEtimestamp}', '{prttEsolicitante}', '{prttEatendente}', '{prttEsolIP}', '{prttEsolPort}', '{prttEmediaType}', '{prttPhoneNumber}', '{AccountIdentifier}', {ar_id}, {linh_id}, '{TipoDirecaoCall}' WHERE NOT EXISTS (SELECT cal_id FROM leitores.tb_whatszap_call_log WHERE call_id = '{prttcallID}' AND call_creator = '{prttcallCreator}' AND call_timestamp = '{prttEtimestamp}' AND call_from = '{prttEsolicitante}' AND call_to = '{prttEatendente}' AND call_from_ip = '{prttEsolIP}' AND call_media_type = '{prttEmediaType}' AND call_phone_number = '{prttPhoneNumber}' AND telefone = '{AccountIdentifier}');"

                                                try:
                                                    db.execute(sqlInsert)
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")

                                                if logSql:
                                                    print("Log callLogs Eventos Participantes", sqlInsert)

                                        else:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) SELECT '{prttcallID}', '{prttcallCreator}', '{prttEtype}', '{prttEtimestamp}', '{prttEsolicitante}', '{prttEatendente}', '{prttEsolIP}', '{prttEsolPort}', '{prttEmediaType}', '{prttPhoneNumber}', '{AccountIdentifier}', {ar_id}, {linh_id}, '{TipoDirecaoCall}' WHERE NOT EXISTS (SELECT cal_id FROM leitores.tb_whatszap_call_log WHERE call_id = '{prttcallID}' AND call_creator = '{prttcallCreator}' AND call_timestamp = '{prttEtimestamp}' AND call_from = '{prttEsolicitante}' AND call_to = '{prttEatendente}' AND call_from_ip = '{prttEsolIP}' AND call_media_type = '{prttEmediaType}' AND call_phone_number = '{prttPhoneNumber}' AND telefone = '{AccountIdentifier}');"

                                            try:
                                                db.execute(sqlInsert)
                                                con.commit()
                                            except:
                                                db.execute("rollback")

                                            if logSql:
                                                print("Log callLogs Eventos", sqlInsert)
                    else:
                        print("\nNÃO LOCALIZADO A CONTA TELEFONICA PRTT", ar_id)

                        return False

            else:

                print("\nLINHA ID NÃO LOCALIZADA OPERAÇÃO CANCELADA", queryLinId[0])

            con.close()

            return True
        else:
            print("\nNÃO LOCALIZADO A CONTA\n")

            return False

    db.close()
    con.close()
