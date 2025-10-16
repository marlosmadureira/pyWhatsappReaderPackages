import json
import os

import requests

from pyBiblioteca import conectBD, somentenumero, grava_log, print_color
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


# def sendDataJsonServer(Dados, type):
#     payload = {'token': APITOKEN, 'action': 'sendWPData', 'type': type, 'jsonData': json.dumps(Dados)}
#     try:
#         print(f'\nEVENTO POST AGUARDE RESPOSTA DO PHP\n')
#         r = requests.post(APILINK, data=payload)
#
#         if r.status_code == 200 and r.text != "" and r.text is not None:
#             Jsondata = json.loads(r.text)
#
#             return Jsondata
#     except requests.exceptions.ConnectionError:
#         print_color(f'\nBuild http Connection Failed {requests.exceptions.ConnectionError}', 31)
#     except Exception as inst:
#         errorData = "{Location: sendDataJsonServer, error: " + str(inst) + ", type: " + type + "}"
#         print_color(f"{errorData}", 31)
def sendDataJsonServer(Dados, type):
    payload = {'token': APITOKEN, 'action': 'sendWPData', 'type': type, 'jsonData': json.dumps(Dados)}
    try:
        print(f'\nEVENTO POST AGUARDE RESPOSTA DO PHP\n')
        r = requests.post(APILINK, data=payload)

        # Debug: Imprimir status e resposta
        # print(f"Status Code: {r.status_code}")
        # print(f"Response Text: {r.text[:500]}")

        if r.status_code == 200:
            response_text = r.text.strip()  # Remove espaços em branco

            if response_text:  # Verifica se não está vazio
                try:
                    Jsondata = json.loads(response_text)
                    return Jsondata
                except json.JSONDecodeError as e:
                    print_color(f'\nErro ao fazer parse do JSON: {e}', 31)
                    print_color(f'Resposta recebida: {response_text[:500]}', 31)
                    return {'status': 'error', 'message': 'Resposta inválida do servidor'}
            else:
                print_color(f'\nResposta vazia do servidor', 31)
                return {'status': 'error', 'message': 'Resposta vazia'}
        else:
            print_color(f'\nStatus code diferente de 200: {r.status_code}', 31)
            return {'status': 'error', 'message': f'Status code: {r.status_code}'}

    except requests.exceptions.Timeout:
        print_color(f'\nTimeout na requisição', 31)
        return {'status': 'error', 'message': 'Timeout'}
    except requests.exceptions.ConnectionError as e:
        print_color(f'\nErro de conexão: {e}', 31)
        return {'status': 'error', 'message': 'Erro de conexão'}
    except Exception as inst:
        errorData = f"{{Location: sendDataJsonServer, error: {str(inst)}, type: {type}}}"
        print_color(errorData, 31)
        return {'status': 'error', 'message': str(inst), 'type': type}