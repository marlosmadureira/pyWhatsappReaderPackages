import requests
import json
import os
from pyBibliotecaV6 import conectBD, print_color
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def sendMessageElement(accessToken, roomId, mensagem):

    mensagemError = f'🤖 IntelliBot \n 🚨 ALERTA DE SISTEMA 🚨 \n {mensagem}'

    url = f"https://cryptochat.com.br/_matrix/client/r0/rooms/{roomId}/send/m.room.message"
    
    post_data = {
        'msgtype': 'm.text',
        'body': mensagemError,
        'format': 'org.matrix.custom.html',
        'formatted_body': mensagemError,
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {accessToken}',
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(post_data))
        response.raise_for_status()  # Raises a HTTPError for bad responses
        result = response.text
    except requests.exceptions.RequestException as e:
        result = f'Erro ao enviar a mensagem para o grupo: {e}'
    
    return {
        'data': result,
        'status': response.status_code if response else 500
    }


def getroomIdElement(Unidade):
    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()

        sqlRoons = f"SELECT tbelementkey.chave FROM interceptacao.tbelementkey WHERE tbelementkey.whatsapp = TRUE AND tbelementkey.unid_id = {Unidade}"

        try:
            db.execute(sqlRoons)
            queryRoons = db.fetchall()
        except Exception as e:
            pass

    print_color(f"\nGrupo de Alerta {queryRoons}", 34)

    db.close()
    con.close()

    return queryRoons
