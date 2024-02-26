from pyBiblioteca import print_color, grava_log

LogGrava = False


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

    if LogGrava:
        grava_log(message_blocks, f'logMessage_{fileName}.txt')

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

    # Qualquer Novo Div Criada Pasta Inserir o Valor
    campos_desejados = ['Call Id', 'Call Creator', 'Type', 'Timestamp', 'From', 'To', 'From Ip',
                        'From Port', 'Media Type']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    call_blocks = call_logs.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    if LogGrava:
        grava_log(call_blocks, f'logCall_{fileName}.txt')

    # Iterar sobre cada bloco de mensagem
    for block in call_blocks:
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
