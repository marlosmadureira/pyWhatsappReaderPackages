import requests
import json
import os
import html
from pyBibliotecaV6 import conectBD, print_color
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Para DEBUG LOCAL
def sendMessageElementOLD(accessToken, roomId, mensagem):
    title = f"🚨ALERTA — WhatsApp🚨"
    body_text = f"{title}\n{mensagem}"
    body_html = f"<strong>{html.escape(title)}</strong><br/><pre>{html.escape(mensagem)}</pre>"

    print_color(f"\n [DEBUG]\n{body_text}",32)

    # Retorna resposta simulada de sucesso
    return {
        'data': '{"simulated": "success"}',
        'status': 200
    }

# Produção
def sendMessageElementOLDS(accessToken, roomId, mensagem):

    mensagemError = f'🤖 IntelliBot 🤖\n 🚨 ALERTA DE SISTEMA 🚨 \n {mensagem}'

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
        response = requests.post(url, headers=headers, data=json.dumps(post_data), timeout=20)
        result_text = response.text
        return {
            'ok': response.ok,
            'status': response.status_code,
            'url': url,
            'roomId': roomId,
            'response_snippet': (result_text or '')[:500]
        }
    except requests.exceptions.RequestException as e:
        return {
            'ok': False,
            'status': 0,
            'url': url,
            'roomId': roomId,
            'error': str(e)
        }
def sendMessageElement(accessToken, roomId, mensagem):
    # title = f"🚨ALERTA — WhatsApp🚨"
    body_text = mensagem  # Texto plano para acessibilidade
    body_html = mensagem  # HTML direto - SEM escape adicional!

    url = f"https://cryptochat.com.br/_matrix/client/r0/rooms/{roomId}/send/m.room.message"
    post_data = {
        'msgtype': 'm.text',
        'body': body_text,
        'format': 'org.matrix.custom.html',
        'formatted_body': body_html,
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {accessToken}',
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(post_data), timeout=20)
        result_text = response.text
        return {
            'ok': response.ok,
            'status': response.status_code,
            'url': url,
            'roomId': roomId,
            'response_snippet': (result_text or '')[:500]
        }
    except requests.exceptions.RequestException as e:
        return {
            'ok': False,
            'status': 0,
            'url': url,
            'roomId': roomId,
            'error': str(e)
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

    print_color(f"Grupo de Alerta {queryRoons}\n", 33)

    db.close()
    con.close()

    return queryRoons
