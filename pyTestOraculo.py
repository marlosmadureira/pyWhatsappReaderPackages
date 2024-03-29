import json

import requests
import re
from bs4 import BeautifulSoup
from time import sleep


def consulta_ips(ips_para_pesquisa):
    #urlApi = f'http://ip-api.com/json/{ips_para_pesquisa}?fields=status,city,lat,lon,isp,query&lang=pt-BR'
    urlApi = f'https://ipinfo.io/{ips_para_pesquisa}/json'

    try:
        r = requests.get(urlApi)

        if r.status_code == 200 and r.text != "" and r.text is not None:
            Jsondata = json.loads(r.text)
            print(Jsondata)
    except requests.exceptions.ConnectionError:
        print('build http connection failed')
    except Exception as inst:
        print(f"Location: sendDataJsonServer, error: {inst}")


def getEvents(value_text):
    # Padrao regex para extrair informações
    padrao = r"Type(\w+)Timestamp(.*?)From(\d+)To(\d+)From Ip(.*?)From Port(\d+)"

    # Procurar todas as correspondências na string
    correspondencias = re.findall(padrao, value_text)

    # Processar cada correspondência
    informacoes = []

    if 'Participants' in value_text:
        Participants = getParticipants(value_text)
    else:
        Participants = None

    for match in correspondencias:

        if 'Type' in match[0]:
            resultado = match[0].split("Type")
            Type = resultado[1]
            MediaType = resultado[0]
        else:
            Type = match[0]
            MediaType = None

        # Criar dicionário com informações
        info = {
            'Type': Type,
            'Timestamp': match[1],
            'From': match[2],
            'To': match[3],
            'From Ip': consulta_ips(match[4]),
            'From Port': match[5],
            'Media Type': MediaType,
            'Participants': Participants
        }

        informacoes.append(info)

    return informacoes


def getParticipants(value_text):
    padrao = r'Phone Number(\d+)State(\w+)Platform(\w+)'
    matches = re.findall(padrao, value_text)

    informacoes_separadas = []
    for match in matches:

        if 'Type' in match[2]:
            resultado = match[2].split("Type")
            Platform = resultado[0].replace("Phone", "")
        else:
            Platform = match[2].replace("Phone", "")

        informacao = {
            'Phone Number': match[0],
            'State': match[1],
            'Platform': Platform
        }
        informacoes_separadas.append(informacao)

    return informacoes_separadas


def parse_html(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')

    request_parameters = parse_request_parameters(soup)
    message_log = parse_message_log(soup)
    call_logs = parse_call_logs(soup)

    print(call_logs)

    # return {
    #     'request_parameters': request_parameters,
    #     'message_log': message_log,
    #     'call_logs': call_logs
    # }


def parse_request_parameters(soup):
    content_div = soup.find('div', id='property-request_parameters')
    data = {}
    if content_div:
        div_tables = content_div.find_all('div', class_='div_table', style='font-weight: bold;', recursive=False)
        for div_table in div_tables:
            # Aqui, modificamos a extração para capturar corretamente a chave e o valor
            key_div = div_table.find('div', style='font-weight: bold; display:table;')
            value_div = key_div.find_next('div', style=lambda
                value: 'font-weight: normal;' in value and 'display:table-cell;' in value)

            if key_div and value_div:
                # Removemos o valor do texto da chave para obter apenas a chave pura
                key = key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip()
                value = value_div.get_text(strip=True)
                data[key] = value
    return data


def clean_html(html_text):
    """Remove tags HTML e espaços extras de uma string HTML."""
    text = re.sub('<[^>]+>', '', html_text)  # Remove tags HTML
    text = re.sub('\s+', ' ', text)  # Substitui múltiplos espaços por um único espaço
    return text.strip()


def parse_message_log(soup):
    message_log_div = soup.find('div', id='property-message_log')
    messages = []

    if message_log_div:
        messages = []
        message_log_div = soup.find('div', id='property-message_log')

        if message_log_div:
            # Ignora a definição do log de mensagem e foca nos registros de mensagens
            message_blocks = message_log_div.find_all('div', class_='div_table')[1:]  # Pula a descrição

            for block in message_blocks:
                message_info = {}
                detail_blocks = block.find_all('div', class_='div_table', recursive=True)

                for detail_block in detail_blocks:
                    key_div = detail_block.find('div', style='font-weight: bold; display:table;')
                    if key_div:
                        value_div = key_div.find_next('div', style=lambda
                            value: 'display:table-cell;' in value if value else False)
                        if value_div:
                            key_text = clean_html(
                                key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
                            value_text = clean_html(value_div.get_text(strip=True))

                            # Evita a sobreposição de chaves, adicionando valores com o mesmo nome de chave
                            if key_text != "Message":
                                message_info[key_text] = value_text

                if message_info and message_info not in messages:
                    messages.append(message_info)

        return messages


def parse_call_logs(soup):
    call_log_div = soup.find('div', id='property-call_logs')
    call_logs = []

    if call_log_div:
        call_blocks = call_log_div.find_all('div', class_='div_table')[1:]  # Pula a descrição dos logs de chamada

        for block in call_blocks:
            call_info = {}
            detail_blocks = block.find_all('div', class_='div_table', recursive=True)

            for detail_block in detail_blocks:
                key_div = detail_block.find('div', style='font-weight: bold; display:table;')
                if key_div:
                    # Procura pelo próximo div que corresponde ao valor, considerando qualquer estilo após "display:table-cell;"
                    value_div = key_div.find_next('div', style=lambda
                        value: 'display:table-cell;' in value if value else False)
                    if value_div:
                        key_text = clean_html(
                            key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
                        value_text = clean_html(value_div.get_text(strip=True))
                        # key_text = key_div.get_text(strip=True)
                        # value_text = " ".join(value_div.stripped_strings).replace('\n', ' ').replace('<br/>', '\n')

                        # Trata a possibilidade de múltiplos eventos dentro de uma única chamada
                        if key_text != "Call":
                            if 'Call Id' in key_text or 'Call Creator' in key_text:
                                call_info[key_text] = value_text
                            elif 'Events' in key_text:
                                call_info[key_text] = getEvents(value_text)

            if call_info and call_info not in call_logs:
                call_logs.append(call_info)

    return call_logs


# Caminho do arquivo HTML (ajustar conforme necessário)
# file_path = "/home/inteligencia/Documentos/Projetos/Python-Projetos/pyWhatsappReaderPackages/arquivos/extracao/1388347178711117/records.html"
# file_path = "/home/inteligencia/Documentos/Projetos/Python-Projetos/pyWhatsappReaderPackages/arquivos/extracao/853013806245227/records.html"
file_path = "/home/inteligencia/Documentos/Projetos/Python-Projetos/pyWhatsappReaderPackages/arquivos/extracao/195219527016517/records.html"

# Processar o arquivo e exibir os resultados
parsed_data = parse_html(file_path)
print(parsed_data)
