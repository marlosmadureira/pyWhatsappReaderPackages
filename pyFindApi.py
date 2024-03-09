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
executaSql = False

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


def sendDataPostgres(Dados, type):
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

        if AccountIdentifier is not None and Unidade is not None:
            if 'DADOS' in type:
                print(Dados)

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

                                print(f"{prttEtype}")


        #     sqlTratamento = f"SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL;"
        #     db.execute(sqlTratamento)
        #     queryTratamento = db.fetchone()
        #
        #     if queryTratamento is not None and queryTratamento[0] > 0:
        #         apli_id = queryTratamento[0]
        #         linh_id = queryTratamento[1]
        #         conta_id = re.sub('[^0-9]', '', queryTratamento[2])
        #
        #         sqlUpdate = f"UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '%s' WHERE conta_zap IS NULL AND apli_id = %s AND linh_id = %s"
        #
        #         if executaSql:
        #             try:
        #                 db.execute(sqlUpdate, (conta_id, apli_id, linh_id))
        #                 con.commit()
        #             except:
        #                 db.execute("rollback")
        #                 pass
        #
        #     sqllinh_id = f"SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbaplicativo_linhafone.conta_zap = '{AccountIdentifier}' GROUP BY tbaplicativo_linhafone.linh_id"
        #
        #     try:
        #         db.execute(sqllinh_id)
        #         queryLinId = db.fetchone()
        #     except:
        #         pass
        #
        #     if queryLinId is not None and queryLinId[0] > 0:
        #
        #         linh_id = queryLinId[0]
        #
        #         sqlexistente = f"SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = {linh_id} AND ar_arquivo = '{FileName}' AND ar_dtgerado = '{DateRange}'"
        #
        #         try:
        #             db.execute(sqlexistente)
        #             queryExiste = db.fetchone()
        #         except:
        #             pass
        #
        #         if queryExiste is None:
        #
        #             sqlInsert = f"INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) SELECT {linh_id}, '{AccountIdentifier}', '{DateRange}', NOW(), '{FileName}', 1, 1, '{EmailAddresses}' RETURNING ar_id;"
        #
        #             if executaSql:
        #                 try:
        #                     db.execute(sqlInsert)
        #                     con.commit()
        #                     result = db.fetchone()
        #                     if result is not None and result[0] is not None:
        #                         ar_id = result[0]
        #                     else:
        #                         ar_id = None
        #                 except:
        #                     db.execute("rollback")
        #                     pass
        #
        #             if ar_id is not None:
        #                 if 'DADOS' in type:
        #                     print('')
        #
        #                 if 'PRTT' in type:
        #                     print('')
        #         else:
        #             print(f"\nARQUIVO EXISTNTE {FileName}")
        #     else:
        #         print(f"\nLINHA NÃO LOCALIZADA OU INTERCEPTADA {AccountIdentifier}\n")
        # else:
        #     print(f"\nNÃO LOCALIZADO A CONTA {AccountIdentifier}\n")

    db.close()
    con.close()
