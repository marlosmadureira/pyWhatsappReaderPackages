from zeep import Client
import json


def sendMessageElement(accessToken, roomId, mensagem):
    # URL do WSDL
    wsdl = 'http://10.115.136.194/endpoint/index.php?wsdl'

    mensagemError = f'BOOT ALERTA DE SISTEMA ERRO DE PROCESSAMENTO ARQUIVO {mensagem}'

    # Criação do cliente SOAP
    client = Client(wsdl=wsdl)

    # Dados a serem enviados
    dados = json.dumps({
        'accessToken': f'{accessToken}',
        'roomId': f'{roomId}',
        'mensagem': f'{mensagemError}'
    })

    # Parâmetros do método
    parametros = {
        'find': dados
    }

    # Definição das credenciais
    credLogin = 'PM&SP_W&BS&RVIC&'  # Substitua pelo seu login
    credPassword = 'B@nd&i r@nt&s'  # Substitua pela sua senha
    client.transport.session.auth = (credLogin, credPassword)

    # Chamada do método sendmessageelement
    resultado = client.service.sendmessageelement(parametros)

    return resultado


def getroomIdElement(Unidade):
    # CPI4 - BAURU
    if Unidade == 43:
        return f'!ihbfspHqeSHyCHnquK:cryptochat.com.br'
    # CPCHOQUE - SAO PAULO
    if Unidade == 33:
        return f'!aaEuhYgIwsgwpOkTEm:cryptochat.com.br'
