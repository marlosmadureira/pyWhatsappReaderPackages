from pyBiblioteca import print_color, grava_log

LogGrava = True


def book_infoReader(address_book_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO BOOK INFO ===========================", 32)

    campos_desejados = ['Symmetric contacts', 'Asymmetric contacts']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    contact_blocks = address_book_info.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    if LogGrava:
        grava_log(contact_blocks, f'logBook_{fileName}.txt')

    # Iterar sobre cada bloco de mensagem
    for block in contact_blocks:
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

            # Obter o texto dentro da tag <div>
            phone_text = field_value_div.get_text(separator='\n')
            # Dividir o texto em uma lista usando quebras de linha
            phone_list = phone_text.split('\n')

            if field_value_div:
                field_value = field_value_div.text.strip()
                field_name = field_name_text.replace(field_value, '').strip()

                if field_name in campos_desejados:
                    data[field_name] = phone_list[1:]

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


def groups_infoReader(groups_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO GROUPS INFO ===========================", 32)

    campos_desejados = ['Picture', 'Linked Media File', 'Thumbnail', 'ID', 'Creation', 'Size', 'Description', 'Subject']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    group_blocks = groups_info.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    if LogGrava:
        grava_log(group_blocks, f'logGroup_{fileName}.txt')

    # Iterar sobre cada bloco de mensagem
    for block in group_blocks:
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


def ncmec_reportsReader(ncmec_reports, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO NCMEC REPORTS ===========================", 32)

    campos_desejados = ['Ncmec Reports Definition', 'NCMEC CyberTip Numbers']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    ncmec_blocks = ncmec_reports.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

    if LogGrava:
        grava_log(ncmec_blocks, f'logNcmec_{fileName}.txt')

    # Iterar sobre cada bloco de mensagem
    for block in ncmec_blocks:
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


def connection_infoReader(connection_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CONNECTION INFO ===========================", 32)

    if DebugMode:
        print(f"{connection_info}")

    print(f"OUT {connection_info}")


def web_infoReader(web_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO WEB INFO ===========================", 32)

    if DebugMode:
        print(f"{web_info}")

    print(f"OUT {web_info}")
