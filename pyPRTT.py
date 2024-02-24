from pyBiblioteca import print_color
def message_logReader(message_log, DebugMode):
    print_color(f"\n=========================== PROCESSANDO MESSAGES LOGS ===========================", 32)

    if DebugMode:
        print(message_log)

    campos_desejados = ['Timestamp', 'Message Id', 'Sender', 'Recipients', 'Sender Ip', 'Sender Port', 'Sender Device',
                        'Type', 'Message Style', 'Message Size']

    # Lista para armazenar todos os registros
    todos_os_registros = []

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
                    data[field_name] = field_value

        if len(data) > 0:
            # Adicionar o registro à lista
            if data not in todos_os_registros:
                todos_os_registros.append(data)

    if DebugMode:
        # Print dos registros
        for registro in todos_os_registros:
            print(registro)

    print(data)

    if data is not None:
        return data
    else:
        return None


def call_logsReader(call_logs, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CALL LOGS ===========================", 32)

    if DebugMode:
        print(call_logs)

    campos_desejados = ['Call Id', 'Call Creator', 'Type', 'Timestamp', 'From', 'To', 'From Ip',
                        'From Port', 'Media Type', 'Events']

    # Lista para armazenar todos os registros
    todos_os_registros = []

    # Encontrar todos os blocos de mensagem
    message_blocks = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

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
                    data[field_name] = field_value

        if len(data) > 0:
                todos_os_registros.append(data)

    if not DebugMode:
        # Print dos registros
        for registro in todos_os_registros:
            print(registro)

    print(data)

    if data is not None:
        return data
    else:
        return None
