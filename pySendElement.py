import requests
import json

def sendMessageElement(accessToken, roomId, mensagem):

    mensagemError = f'🤖 IntelliBot \n 🚨 ALERTA DE SISTEMA 🚨 \n ERRO DE PROCESSAMENTO ARQUIVO WHATSAPP {mensagem}'

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
    
    # CPI4 - BAURU
    if int(Unidade) == int(43):
        return f'!ihbfspHqeSHyCHnquK:cryptochat.com.br'
    # CPCHOQUE - SAO PAULO
    if int(Unidade) == int(33):
        return f'!aaEuhYgIwsgwpOkTEm:cryptochat.com.br'

    return None
