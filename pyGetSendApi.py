import json
import os
import requests
import uuid
from pyBiblioteca import conectBD, somentenumero, grava_log, print_color
from dotenv import load_dotenv
from requests.exceptions import Timeout

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
    request_id = str(uuid.uuid4())

    payload = { 'token': APITOKEN, 'action': 'sendWPData', 'type': type, 'jsonData': json.dumps(Dados, ensure_ascii=False), 'request_id': request_id, }

    print(f'\nEVENTO POST (request_id={request_id}) | AGUARDE RESPOSTA DO PHP')

    # Config retry apenas para TIMEOUT
    max_retries = 3
    base_delay = 1.0  # segundos

    for attempt in range(max_retries):
        try:
            r = requests.post(
                APILINK,
                data=payload,
                timeout=(10, 120),  # connect, read
            )

            response_text = (r.text or '').strip()
            if r.status_code != 200:
                return {
                    'ok': False,
                    'status': r.status_code,
                    'error': {
                        'code': 'HTTP_STATUS_NOT_200',
                        'message': f'Status code: {r.status_code}',
                    },
                    'context': {
                        'request_id': request_id,
                        'url': APILINK,
                        'response_snippet': response_text[:1500],
                    }
                }

            if not response_text:
                return {
                    'ok': False,
                    'status': 200,
                    'error': {'code': 'EMPTY_RESPONSE', 'message': 'Resposta vazia do servidor'},
                    'context': {'request_id': request_id, 'url': APILINK}
                }

            try:
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                return {
                    'ok': False,
                    'status': 200,
                    'error': {'code': 'JSON_DECODE_ERROR', 'message': str(e)},
                    'context': {
                        'request_id': request_id,
                        'url': APILINK,
                        'response_snippet': response_text[:1500],
                    }
                }

        except Timeout as e:
            print(f"Tentativa {attempt + 1}/{max_retries} falhou por TIMEOUT: {str(e)[:100]}")

            if attempt < max_retries - 1:  # Não é última tentativa
                delay = base_delay * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                print(f"Aguardando {delay:.1f}s antes da próxima tentativa...")
                time.sleep(delay)
                continue
            else:
                # Última tentativa falhou → retorna TIMEOUT
                return {
                    'ok': False,
                    'status': 0,
                    'error': {'code': 'TIMEOUT', 'message': f'Timeout após {max_retries} tentativas'},
                    'context': {'request_id': request_id, 'url': APILINK, 'tentativas': max_retries}
                }

        except requests.exceptions.ConnectionError as e:
            return {
                'ok': False,
                'status': 0,
                'error': {'code': 'CONNECTION_ERROR', 'message': str(e)},
                'context': {'request_id': request_id, 'url': APILINK}
            }

        except Exception as e:
            return {
                'ok': False,
                'status': 0,
                'error': {'code': 'UNEXPECTED_EXCEPTION', 'message': str(e)},
                'context': {'request_id': request_id, 'url': APILINK, 'where': 'sendDataJsonServer', 'type': type}
            }

# Para DEBUG LOCAL
def sendDataJsonServerOLDS(Dados, type, max_retries=1, retry_delay=2):
    payload = {'token': APITOKEN, 'action': 'sendWPData', 'type': type, 'jsonData': json.dumps(Dados)}
    attempt = 0
    while True:
        attempt += 1
        try:
            ts = datetime.datetime.now().isoformat()
            print(f"\n[{ts}] EVENTO POST (attempt {attempt}) AGUARDE RESPOSTA DO PHP — fileType={type}\n")
            # Log payload summary (sem token)
            safe_payload = dict(payload)
            if 'token' in safe_payload:
                safe_payload['token'] = '***'
            print(f"Payload (resumido): {safe_payload}\n")

            r = requests.post(APILINK, data=payload, timeout=60)

            print(f"HTTP Status: {r.status_code}")
            print(f"Resposta (primeiros 2000 chars): {repr(r.text[:2000])}\n")

            if r.status_code == 200:
                response_text = (r.text or "").strip()
                if not response_text:
                    return {'status': 'error', 'message': 'Resposta vazia', 'http_status': r.status_code, 'raw': response_text}
                try:
                    Jsondata = json.loads(response_text)
                    # normalize response into consistent shape:
                    return {'status': 'ok', 'body': Jsondata, 'http_status': r.status_code, 'raw': response_text}
                except json.JSONDecodeError as e:
                    print_color(f'\nErro ao fazer parse do JSON: {e}', 31)
                    print_color(f'Resposta recebida (raw): {response_text[:5000]}', 31)
                    return {'status': 'error', 'message': 'Resposta inválida do servidor', 'http_status': r.status_code, 'raw': response_text}
            else:
                print_color(f'\nStatus code diferente de 200: {r.status_code}', 31)
                return {'status': 'error', 'message': f'Status code: {r.status_code}', 'http_status': r.status_code, 'raw': r.text}

        except requests.exceptions.Timeout:
            print_color(f'\nTimeout na requisição (attempt {attempt})', 31)
            if attempt <= max_retries:
                time.sleep(retry_delay)
                continue
            return {'status': 'error', 'message': 'Timeout'}
        except requests.exceptions.ConnectionError as e:
            print_color(f'\nErro de conexão: {e}', 31)
            if attempt <= max_retries:
                time.sleep(retry_delay)
                continue
            return {'status': 'error', 'message': 'Erro de conexão', 'exception': str(e)}
        except Exception as inst:
            errorData = f"{{Location: sendDataJsonServer, error: {str(inst)}, type: {type}}}"
            print_color(errorData, 31)
            return {'status': 'error', 'message': str(inst), 'type': type}
