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

    # print(f"OUT {allRegistros}")

    if allRegistros is not None:
        return allRegistros
    else:
        return None


def newCallInfo():
    return {
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


def process_div_table(alldivs):
    all_calls_info = []
    newCall = newCallInfo()

    for dev in alldivs:
        text = dev.text.strip()
        if 'Type' in text:
            if newCall['Type'] is not None:
                all_calls_info.append(newCall)
            newCall = newCallInfo()
            newCall['Type'] = text.replace("Type", "").strip()

        if 'Timestamp' in text:
            newCall['Timestamp'] = text.replace("Timestamp", "").strip()

        if 'From' in text:
            newCall['From'] = text.replace("From", "").strip()

        if 'To' in text:
            newCall['To'] = text.replace("To", "").strip()

        if 'From Ip' in text:
            newCall['From_Ip'] = text.replace("From Ip", "").strip()

        if 'From Port' in text:
            newCall['From_Port'] = text.replace("From Port", "").strip()

        if 'Media Type' in text:
            newCall['Media_Type'] = text.replace("Media Type", "").strip()

        if 'Call Id' in text:
            newCall['Call_id'] = text.replace("Call Id", "").strip()

        if 'Call Creator' in text:
            newCall['Call_Creator'] = text.replace("Call Creator", "").strip()

    return all_calls_info


def call_logsReader(call_logs, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)

    if DebugMode:
        print(call_logs)

    alldivtables = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    all_calls_info = process_div_table(alldivtables)

    grava_log(all_calls_info, 'LogCall.txt')

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
