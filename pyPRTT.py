from pyBiblioteca import print_color, grava_log, remover_espacos_regex


def message_logReader(message_log, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO MESSAGES LOGS ===========================", 32)

    if DebugMode:
        print(message_log)

    # Qualquer Novo Div Criada Pasta Inserir o Valor
    campos_desejados = ['Timestamp', 'Message Id', 'Sender', 'Recipients', 'Sender Ip', 'Sender Port', 'Sender Device',
                        'Type', 'Message Style', 'Message Size']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    message_blocks = message_log.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    # Iterar sobre cada bloco de mensagem
    for block in message_blocks:
        # Dicionário para armazenar os dados de um registro
        data = {}

        # Encontrar todos os campos dentro de um bloco
        fields = block.find_all("div", class_="div_table", style="font-weight: bold;")

        # Iterar sobre cada campo e extrair informações
        for field in fields:
            field_name_div = field.find("div", style="font-weight: bold; display:table;")
            field_name_text = field_name_div.text.strip() if field_name_div else ""

            field_value_div = field.find("div",
                                         style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
            if field_value_div:
                field_value = field_value_div.text.strip()
                field_name = field_name_text.replace(field_value, '').strip()
                if field_name in campos_desejados:
                    data[remover_espacos_regex(field_name)] = field_value

                    # grava_log(remover_espacos_regex(field_name), 'LogKeyMessage.txt')

        if len(data) > 0:
            # Adicionar o registro à lista
            if data not in allRegistros:
                allRegistros.append(data)

    if DebugMode:
        # Print dos registros
        for registro in allRegistros:
            print(registro)

    print(f"OUT {allRegistros}")

    if allRegistros is not None:
        return allRegistros
    else:
        return None


def process_div_table(div_table):
    # Inicializando o dicionário para armazenar as informações da chamada
    call_info = {
        'Call_id': None,
        'Call_Creator': None,
        'Type': None,
        'Timestamp': None,
        'To': None,
        'From': None,
        'From_Ip': None,
        'From_Port': None,
        'Media_Type': None
    }

    # Extração e armazenamento das informações no dicionário
    for content in div_table.contents:
        text = content.text.strip()  # Obtendo o texto do conteúdo atual

        if 'Call Id' in text:
            call_info['Call_id'] = text.replace("Call Id", "").strip()

        elif 'Call Creator' in text:
            call_info['Call_Creator'] = text.replace("Call Creator", "").strip()

        elif 'Type' in text:
            call_info['Type'] = text.replace("Type", "").strip()

        elif 'Timestamp' in text:
            call_info['Timestamp'] = text.replace("Timestamp", "").strip()

        elif 'To' in text:
            call_info['To'] = text.replace("To", "").strip()

        elif 'From' in text:
            call_info['From'] = text.replace("From", "").strip()

        elif 'From Ip' in text:
            call_info['From_Ip'] = text.replace("From Ip", "").strip()

        elif 'From Port' in text:
            call_info['From_Port'] = text.replace("From Port", "").strip()

        elif 'Media Type' in text:
            call_info['Media_Type'] = text.replace("Media Type", "").strip()

    return call_info

def call_logsReader(call_logs, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)

    if DebugMode:
        print(call_logs)

    alldivtables = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")
    all_calls_info = [process_div_table(div_table) for div_table in alldivtables]

    print(all_calls_info)

# def call_logsReader(call_logs, fileName, DebugMode):
#     print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)
#
#     if DebugMode:
#         print(call_logs)
#
#     # Qualquer Novo Div Criada Pasta Inserir o Valor
#     campos_desejados = ['Call Id', 'Call Creator', 'Events'] #'Type', 'Timestamp', 'From', 'To', 'From Ip', 'From Port', 'Media Type',
#
#     # Lista para armazenar todos os registros
#     allRegistros = []
#
#     # Encontrar todos os blocos de mensagem
#     call_blocks = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")
#
#     # Iterar sobre cada bloco de mensagem
#     for block in call_blocks:
#         # Dicionário para armazenar os dados de um registro
#         data = {}
#
#         # Encontrar todos os campos dentro de um bloco
#         fields = block.find_all("div", class_="div_table", style="font-weight: bold;")
#
#         # Iterar sobre cada campo e extrair informações
#         for field in fields:
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 field_name = field_name_text.replace(field_value, '').strip()
#
#                 if field_name in campos_desejados:
#                     data[remover_espacos_regex(field_name)] = field_value
#
#                     if 'Events' in field_name:
#                         print(f"\n{field_value_div}")
#
#                     # grava_log(remover_espacos_regex(field_name), 'LogKeyCall.txt')
#
#         if len(data) > 0:
#             allRegistros.append(data)
#
#     if DebugMode:
#         # Print dos registros
#         for registro in allRegistros:
#             print(registro)
#
#     print(f"OUT {allRegistros}")
#
#     if allRegistros is not None:
#         return allRegistros
#     else:
#         return None
