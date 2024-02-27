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


def call_logsReader(call_logs, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)

    if DebugMode:
        print(call_logs)

    alldivtables = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    for div_table in alldivtables:

        Call_id = None
        Call_Creator = None
        Type = None
        Timestamp = None
        To = None
        From = None
        From_Ip = None
        From_Port = None
        Media_Type = None

        if 'Call Id' in div_table:
            Call_id = div_table.text
            Call_id = Call_id.replace("Call Id", "")

            print(f"{Call_id}")

        if 'Call Creator' in div_table:
            Call_Creator = div_table.text
            Call_Creator = Call_Creator.replace("Call Creator", "")

            print(f"{Call_Creator}")

        if 'Type' in div_table:
            Type = div_table.text
            Type = Type.replace("Type", "")

            print(f"{Type}")

        if 'Timestamp' in div_table:
            Timestamp = div_table.text
            Timestamp = Timestamp.replace("Timestamp", "")

            print(f"{Timestamp}")

        if 'To' in div_table:
            To = div_table.text
            To = To.replace("To", "")

            print(f"{To}")

        if 'From' in div_table:
            From = div_table.text
            From = From.replace("From", "")

            print(f"{From}")

        if 'From Ip' in div_table:
            From_Ip = div_table.text
            From_Ip = From_Ip.replace("From Ip", "")

            print(f"{From_Ip}")

        if 'From Port' in div_table:
            From_Port = div_table.text
            From_Port = From_Port.replace("From Port", "")

            print(f"{From_Port}")

        if 'Media Type' in div_table:
            Media_Type = div_table.text
            Media_Type = Media_Type.replace("Media Type", "")

            print(f"{Media_Type}")

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
