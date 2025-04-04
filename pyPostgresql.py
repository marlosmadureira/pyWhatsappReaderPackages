import os
import json
import time
import requests

from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport

from pyBiblioteca import conectBD, grava_log, somentenumero, print_color
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

APILINK = os.getenv("APILINK")
APITOKEN = os.getenv("APITOKEN")

executaSql = True
logSql = False

def sql_grupos():
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        sqlDados = f"SELECT tbelementkey.chave FROM interceptacao.tbelementkey WHERE tbelementkey.whatsapp = TRUE"
        db.execute(sqlDados)
        queryDados = db.fetchall()

    db.close()
    con.close()

    return queryDados

def sendMessageElement(mensagem, accessToken):
    listaroomId = sql_grupos()

    mensagem = f'🤖 IntelliBot \n🚨 ALERTA DE SISTEMA 🚨\n{mensagem}. Caso Ofício Não Pertencer Operação Por Favor Desconsiderar QTC'

    login = 'IntelliBot'
    password = 'IntelliBot'
    cpf = '28617756888'

    credLogin = "PM&SP_W&BS&RVIC&"
    credPassword = "B@nd&i r@nt&s"

    # Definindo o WSDL e configurando o cliente SOAP
    wsdl = 'http://10.10.10.194/endpoint/index.php?wsdl'
    session = requests.Session()
    session.auth = HTTPBasicAuth(credLogin, credPassword)
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, transport=transport)

    for roomId in listaroomId:
        # Preparando os dados
        dados = json.dumps({
            'accessToken': accessToken,
            'roomId': roomId[0],
            'mensagem': mensagem
        })

        # Parâmetros
        parametros = {
            'login': login,
            'password': password,
            'cpf': cpf,
            'find': dados
        }

        time.sleep(2)

        client.service.sendmessageelement(parametros)

def saveResponse(telefone, unidade):
    # Obter a data atual
    obje_dtinicio = datetime.now()

    # Somar 15 dias
    obje_dtfim = obje_dtinicio + timedelta(days=15)

    # Subtrair 2 dias do resultado
    obje_dtprorr = obje_dtfim - timedelta(days=2)

    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        sqlDadosObj = f"SELECT tbobje_intercepta.obje_id, tbobje_intercepta.linh_id FROM interceptacao.tbobje_intercepta WHERE tbobje_intercepta.opra_id  = 28 AND tbobje_intercepta.obje_tipo <='2' AND (tbobje_intercepta.obje_proximo is null OR tbobje_intercepta.obje_proximo in (SELECT obje_id FROM interceptacao.tbobje_intercepta WHERE obje_dtinicio is null)) AND tbobje_intercepta.ofic_id > 0 AND ( tbobje_intercepta.interceptado = 'I' AND tbobje_intercepta.obje_dtfim is null) AND tbobje_intercepta.unid_id = {unidade} AND ( tbobje_intercepta.obje_id IN (SELECT tbobje_intercepta.obje_id FROM interceptacao.tbobje_intercepta, linha_imei.tblinhafone, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tblinhafone.linh_id AND tblinhafone.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.unid_id = {unidade} AND tbaplicativo_linhafone.conta_zap ILIKE '%{telefone}%'))"

        db.execute(sqlDadosObj)
        queryDadosObj = db.fetchone()

        if queryDadosObj is not None:
            Obje_id = queryDadosObj[0]
            Linh_id = queryDadosObj[1]

            if Obje_id is not None and Linh_id is not None:
                sqlUpdate = f"UPDATE interceptacao.tbobje_intercepta SET obje_dtinicio = %s, obje_dtprorr = %s, obje_dtfim = %s WHERE tbobje_intercepta = %s"
                try:
                    db.execute(sqlUpdate, (obje_dtinicio.strftime("%Y-%m-%d %H:%M:%S"), obje_dtprorr.strftime("%Y-%m-%d %H:%M:%S"), obje_dtfim.strftime("%Y-%m-%d %H:%M:%S"), Obje_id))
                    con.commit()
                    print_color(f"{db.query}", 32)
                except:
                    print_color(f"{db.query}", 31)
                    con.rollback()
                    pass
            else:
                print_color(f"Conta Não Localizada Para {telefone} Unidade {unidade}", 31)
        else:
            print_color(f"Conta Não Localizada Para {telefone} Unidade {unidade} {sqlDadosObj}", 31)

    db.close()
    con.close()

def find_unidade_postgres(Unidade):
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        if Unidade is not None and Unidade != "":
            sqlFind = f"SELECT unid_nome FROM sistema.tbunidade WHERE tbunidade.unid_id = {Unidade};"
        else:
            sqlFind = f"SELECT unid_nome FROM sistema.tbunidade WHERE tbunidade.unid_id = 1;"

        db.execute(sqlFind)
        queryFind = db.fetchone()

        if queryFind is not None:
            Unidade = queryFind[0].replace("-", "")
        else:
            Unidade = None

    db.close()
    con.close()

    return Unidade

def listaProcessamento(source, Unidade):
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        datanow = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        sqlInsert = f"INSERT INTO leitores.tb_whatszap_processamento (sourcefile, unidade, datanow) VALUES (%s, %s, %s)";

        try:
            db.execute(sqlInsert, (source, Unidade, datanow))
            con.commit()
        except:
            db.execute("rollback")
            pass

    db.close()
    con.close()

def roolBackPostgres(ar_id):
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_agenda WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 1 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_conexaoinfo WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 2 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_grupoinfo WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 3 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_iptime WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 4 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_weinfo WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 5 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_deviceinfo WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 6 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_index_zapcontatos_new WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 7 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_call_log WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 8", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_index_zapcontatos WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 9 ", sqlDelete)

        sqlDelete = f"DELETE FROM leitores.tb_whatszap_arquivo WHERE ar_id = %s"
        if executaSql:
            try:
                db.execute(sqlDelete, (ar_id))
                con.commit()
            except:
                db.execute("rollback")
                pass

        if logSql:
            print(f"Delete 10 ", sqlDelete)

    db.close()
    con.close()


def sendDataPostgres(Dados, type, DebugMode, Out, fileName):
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

            if executaSql:
                try:
                    db.execute(sqlTratamento)
                    queryTratamento = db.fetchone()
                except:
                    pass

            if logSql:
                print(f"Log 1 ", sqlTratamento)
                grava_log(sqlTratamento, f'log_1.txt')

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

                    if logSql:
                        print(f"Log 2 ", db.query)
                        grava_log(db.query, f'log_2.txt')

            sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"

            if executaSql:
                try:
                    db.execute(sqllinh_id)
                    queryLinId = db.fetchone()
                except:
                    pass

                if logSql:
                    print(f"Log 3 ", db.query)
                    grava_log(db.query, f'log_3.txt')

            if queryLinId is not None and queryLinId[0] > 0:

                linh_id = queryLinId[0]

                sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}'"

                if executaSql:
                    try:
                        db.execute(sqlexistente)
                        queryExiste = db.fetchone()
                    except:
                        pass

                    if logSql:
                        print(f"Log 4 ", db.query)
                        grava_log(db.query, f'log_4.txt')

                if queryExiste is None:

                    if 'DADOS' in type:
                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) SELECT {linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1, '{EmailAddresses}' RETURNING ar_id;"
                    else:
                        sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) SELECT {linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 0, 1, '{EmailAddresses}' RETURNING ar_id;"

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

                        if logSql:
                            print(f"Log 5 ", db.query)
                            grava_log(db.query, f'log_5.txt')

                    if ar_id is not None:
                        if 'DADOS' in type:

                            if Dados['Dados'].get('EmailAddresses'):
                                EmailAddresses = Dados['Dados'].get('EmailAddresses')

                                sqlUpdate = f"UPDATE leitores.tb_whatszap_arquivo SET ar_email_addresses = '%s' WHERE ar_id = %s"

                                if executaSql:
                                    try:
                                        db.execute(sqlUpdate, (EmailAddresses, ar_id))
                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

                                    if logSql:
                                        print("Log 6 ", db.query)
                                        grava_log(db.query, f'log_6.txt')

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
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) VALUES ('%s', '%s', '%s', %s, %s)";

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    dadoIPAddress, dadoTime, AccountIdentifier, ar_id, linh_id))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                                if logSql:
                                                    print("Log 7 ", db.query)
                                                    grava_log(db.query, f'log_7.txt')
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

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_conexaoinfo WHERE servicestart = '%s' AND devicetype = '%s' AND appversion = '%s' AND deviceosbuildnumber = '%s' AND telefone = '%s');"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert,
                                                   (dadoServiceStart, dadoDeviceType, dadoAppVersion,
                                                    dadoDeviceOSBuildNumber, dadoConnectionState, dadoOnlineSince,
                                                    dadoPushName, dadoLastSeen, AccountIdentifier, ar_id, linh_id,
                                                    dadoServiceStart, dadoDeviceType, dadoAppVersion,
                                                    dadoDeviceOSBuildNumber, AccountIdentifier))
                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

                                    if logSql:
                                        print(f"Log 8 ", db.query)
                                        grava_log(db.query, f'log_8.txt')

                                if logSql:
                                    print("Log Conexao ", sqlInsert)

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

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', %s, %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_weinfo WHERE we_version = '%s' AND we_platform = '%s' AND telefone = '%s');"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert, (
                                        dadoVersion, dadoPlatform, dadoOnlineSince, dadoInactiveSince,
                                        AccountIdentifier, ar_id, linh_id, dadoVersion, dadoPlatform,
                                        AccountIdentifier))
                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

                                    if logSql:
                                        print(f"Log 9 ", db.query)
                                        grava_log(db.query, f'log_9.txt')

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

                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '%s' AND creation = '%s' AND id_msg = '%s' AND telefone = '%s');"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        dadoTipoGroup, pathFile, dadoThumbnail, dadoID, dadoCreation,
                                                        dadoSize, dadoDescription, dadoSubject, AccountIdentifier,
                                                        ar_id,
                                                        dadoPicture, linh_id, dadoTipoGroup, dadoCreation, dadoID,
                                                        AccountIdentifier))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                                if logSql:
                                                    print(f"Log 10 ", db.query)
                                                    grava_log(db.query, f'log_10.txt')

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

                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '%s' AND creation = '%s' AND id_msg = '%s' AND telefone = '%s');"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        dadoTipoGroup, pathFile, dadoThumbnail, dadoID, dadoCreation,
                                                        dadoSize, dadoDescription, dadoSubject, AccountIdentifier,
                                                        ar_id,
                                                        dadoPicture, linh_id, dadoTipoGroup, dadoCreation, dadoID,
                                                        AccountIdentifier))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                                if logSql:
                                                    print(f"Log 11 ", db.query)
                                                    grava_log(db.query, f'log_11.txt')

                                    if Out:
                                        print(f"{ParticipatingGroups}")

                            if Dados['Dados'].get('addressBookInfo'):
                                if Dados['Dados']['addressBookInfo'][0].get('Symmetriccontacts'):
                                    symmetricContacts = Dados['Dados']['addressBookInfo'][0]['Symmetriccontacts']
                                    if len(symmetricContacts) > 0:
                                        for contacts in symmetricContacts:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', %s, %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '%s' AND ag_tipo = '%s' AND telefone = '%s');"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                    contacts, 'S', AccountIdentifier, ar_id, linh_id, contacts, 'S',
                                                    AccountIdentifier))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                                if logSql:
                                                    print(f"Log 12 ", db.query)
                                                    grava_log(db.query, f'log_12.txt')

                                    if Out:
                                        print(f"{symmetricContacts}")

                                if Dados['Dados']['addressBookInfo'][0].get('Asymmetriccontacts'):
                                    asymmetricContacts = Dados['Dados']['addressBookInfo'][0]['Asymmetriccontacts']
                                    if len(asymmetricContacts) > 0:
                                        for contacts in asymmetricContacts:
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', %s, %s WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '%s' AND ag_tipo = '%s' AND telefone = '%s');"

                                            if executaSql:
                                                try:
                                                    db.execute(sqlInsert, (
                                                        contacts, 'A', AccountIdentifier, ar_id, linh_id, contacts, 'A',
                                                        AccountIdentifier))
                                                    con.commit()
                                                except:
                                                    db.execute("rollback")
                                                    pass

                                                if logSql:
                                                    print(f"Log 13 ", db.query)
                                                    grava_log(db.query, f'log_13.txt')

                                    if Out:
                                        print(f"{asymmetricContacts}")

                            # FALTA AMOSTRA
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

                                # if logSql:
                                #     print(f"Log 14 ", db.query)
                                #     grava_log(db.query, f'log_14.txt')

                                if Out:
                                    print(f"{Dados['Dados'].get('ncmecReportsInfo')}")

                            # FALTA AMOSTRA
                            if Dados['Dados'].get('smallmediumbusinessinfo'):
                                smallMediumBusiness = Dados['Dados']['smallmediumbusinessinfo']

                                print(f"FALTA PROGRAMAR LOGICA GRAVAR BANCO")

                                # if logSql:
                                #     print(f"Log 15 ", db.query)
                                #     grava_log(db.query, f'log_15.txt')

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

                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_deviceinfo (dev_appversion, dev_osversion, dev_buildnumber, dev_manufacturer, dev_devicemodel, ar_id, linh_id, telefone) SELECT '%s', '%s', '%s', '%s', '%s', %s, %s, '%s' WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_deviceinfo WHERE dev_appversion = '%s' AND dev_osversion = '%s' AND dev_buildnumber = '%s' AND telefone = '%s');"

                                if executaSql:
                                    try:
                                        db.execute(sqlInsert, (
                                            AppVersion, OSVersion, OSBuildNumber, DeviceManufacturer, DeviceModel,
                                            ar_id,
                                            linh_id, AccountIdentifier, AppVersion, OSVersion, OSBuildNumber,
                                            AccountIdentifier))
                                        con.commit()
                                    except:
                                        db.execute("rollback")
                                        pass

                                    if logSql:
                                        print(f"Log 16 ", db.query)
                                        grava_log(db.query, f'log_16.txt')

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
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', '%s', '%s', %s, %s"

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

                                                if logSql:
                                                    print(f"Log 17 ", db.query)
                                                    grava_log(db.query, f'log_17.txt')

                                        else:
                                            TipoDirecaoMsg = "Recebeu";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', '%s', '%s', %s, %s"

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

                                                if logSql:
                                                    print(f"Log 18 ", db.query)
                                                    grava_log(db.query, f'log_18.txt')
                                    else:
                                        if prttSender == AccountIdentifier:
                                            TipoDirecaoMsg = "Enviou";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', '%s', '%s', %s, %s"

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

                                                if logSql:
                                                    print(f"Log 19 ", db.query)
                                                    grava_log(db.query, f'log_19.txt')

                                        else:
                                            TipoDirecaoMsg = "Recebeu";
                                            sqlInsert = f"INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', '%s', '%s', %s, %s"

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

                                                if logSql:
                                                    print(f"Log 20 ", db.query)
                                                    grava_log(db.query, f'log_20.txt')

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

                                                    sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s'"

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

                                                        if logSql:
                                                            print(f"Log 21 ", db.query)
                                                            grava_log(db.query, f'log_21.txt')

                                            else:
                                                sqlInsert = f"INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s'"

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

                                                    if logSql:
                                                        print(f"Log 22 ", db.query)
                                                        grava_log(db.query, f'log_22.txt')

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
