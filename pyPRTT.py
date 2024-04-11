import re

from pyBiblioteca import print_color, clean_html, remover_espacos_regex


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
            'From Ip': match[4],
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


def message_logReader(message_log, fileName, DebugMode, Out, tag1, tag2, tag3, tag4):

    if message_log is not None:

        if DebugMode:
            print(message_log)

        messages = []

        # Ignora a definição do log de mensagem e foca nos registros de mensagens
        message_blocks = message_log.find_all('div', class_=f"{tag1}")[1:]  # Pula a descrição

        for block in message_blocks:
            message_info = {}
            detail_blocks = block.find_all('div', class_=f"{tag1}", recursive=True)

            for detail_block in detail_blocks:
                key_div = detail_block.find('div', class_=f"{tag2}")
                if key_div:
                    value_div = key_div.find_next('div', class_=lambda value: f"{tag4}" in value if value else False)
                    if value_div:
                        key_text = clean_html(
                            key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
                        value_text = clean_html(value_div.get_text(strip=True))

                        # Evita a sobreposição de chaves, adicionando valores com o mesmo nome de chave
                        if key_text != "Message":
                            message_info[remover_espacos_regex(key_text)] = value_text

            if message_info and message_info not in messages:
                messages.append(message_info)

        if Out:
            print_color(f"\n=========================== PROCESSANDO MESSAGES LOGS ===========================", 32)

            print(f"OUT {messages}")

        if len(messages) > 0:
            return messages
        else:
            return None
    else:
        return None


# def message_logReader(message_log, fileName, DebugMode, Out):
#     if message_log is not None:
#
#         if DebugMode:
#             print(message_log)
#
#         messages = []
#
#         # Ignora a definição do log de mensagem e foca nos registros de mensagens
#         message_blocks = message_log.find_all('div', class_='div_table')[1:]  # Pula a descrição
#
#         for block in message_blocks:
#             message_info = {}
#             detail_blocks = block.find_all('div', class_='div_table', recursive=True)
#
#             for detail_block in detail_blocks:
#                 key_div = detail_block.find('div', style='font-weight: bold; display:table;')
#                 if key_div:
#                     value_div = key_div.find_next('div', style=lambda
#                         value: 'display:table-cell;' in value if value else False)
#                     if value_div:
#                         key_text = clean_html(
#                             key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
#                         value_text = clean_html(value_div.get_text(strip=True))
#
#                         # Evita a sobreposição de chaves, adicionando valores com o mesmo nome de chave
#                         if key_text != "Message":
#                             message_info[remover_espacos_regex(key_text)] = value_text
#
#             if message_info and message_info not in messages:
#                 messages.append(message_info)
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO MESSAGES LOGS ===========================", 32)
#
#             print(f"OUT {messages}")
#
#         if len(messages) > 0:
#             return messages
#         else:
#             return None
#     else:
#         return None
#


def call_logsReader(call_log_div, fileName, DebugMode, Out, tag1, tag2, tag3, tag4):

    if call_log_div is not None:

        if DebugMode:
            print(call_log_div)

        call_logs = []

        call_blocks = call_log_div.find_all('div', class_=f"{tag1}")[1:]  # Pula a descrição dos logs de chamada

        for block in call_blocks:
            call_info = {}
            detail_blocks = block.find_all('div', class_=f"{tag1}", recursive=True)

            for detail_block in detail_blocks:
                key_div = detail_block.find('div', class_=f"{tag2}")
                if key_div:
                    # Procura pelo próximo div que corresponde ao valor, considerando qualquer estilo após "display:table-cell;"
                    value_div = key_div.find_next('div', class_=lambda value: f"{tag4}" in value if value else False)
                    if value_div:
                        key_text = clean_html(
                            key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
                        value_text = clean_html(value_div.get_text(strip=True))

                        # Trata a possibilidade de múltiplos eventos dentro de uma única chamada
                        if key_text != "Call":
                            if 'Call Id' in key_text or 'Call Creator' in key_text:
                                call_info[remover_espacos_regex(key_text)] = value_text
                            elif 'Events' in key_text:
                                call_info[remover_espacos_regex(key_text)] = getEvents(value_text)

            if call_info and call_info not in call_logs:
                call_logs.append(call_info)

        if Out:
            print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)

            print(f"OUT {call_logs}")

        if len(call_logs) > 0:
            return call_logs
        else:
            return None
    else:
        return None


# def call_logsReader(call_log_div, fileName, DebugMode, Out):
#     if call_log_div is not None:
#
#         if DebugMode:
#             print(call_log_div)
#
#         call_logs = []
#
#         call_blocks = call_log_div.find_all('div', class_='div_table')[1:]  # Pula a descrição dos logs de chamada
#
#         for block in call_blocks:
#             call_info = {}
#             detail_blocks = block.find_all('div', class_='div_table', recursive=True)
#
#             for detail_block in detail_blocks:
#                 key_div = detail_block.find('div', style='font-weight: bold; display:table;')
#                 if key_div:
#                     # Procura pelo próximo div que corresponde ao valor, considerando qualquer estilo após "display:table-cell;"
#                     value_div = key_div.find_next('div', style=lambda
#                         value: 'display:table-cell;' in value if value else False)
#                     if value_div:
#                         key_text = clean_html(
#                             key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
#                         value_text = clean_html(value_div.get_text(strip=True))
#                         # key_text = key_div.get_text(strip=True)
#                         # value_text = " ".join(value_div.stripped_strings).replace('\n', ' ').replace('<br/>', '\n')
#
#                         # Trata a possibilidade de múltiplos eventos dentro de uma única chamada
#                         if key_text != "Call":
#                             if 'Call Id' in key_text or 'Call Creator' in key_text:
#                                 call_info[remover_espacos_regex(key_text)] = value_text
#                             elif 'Events' in key_text:
#                                 call_info[remover_espacos_regex(key_text)] = getEvents(value_text)
#
#             if call_info and call_info not in call_logs:
#                 call_logs.append(call_info)
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)
#
#             print(f"OUT {call_logs}")
#
#         if len(call_logs) > 0:
#             return call_logs
#         else:
#             return None
#     else:
#         return None
