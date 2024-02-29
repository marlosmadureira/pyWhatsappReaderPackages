from pyBiblioteca import print_color, remover_espacos_regex, clean_html


def emails_infoReader(emails_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO EMAIL ===========================", 32)

    if DebugMode:
        print(f"{emails_info}")

    data = {}

    fields = emails_info.find_all("div", class_="div_table", style="font-weight: bold;")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()
            data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None


def ip_addresses_infoReader(ip_addresses_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO IP ADDRESS ===========================", 32)

    if DebugMode:
        print(f"{ip_addresses_info}")

    data = []
    ipinfo = {}

    ipblocks = ip_addresses_info.find_all('div', class_='div_table')[1:]  # Pula a descrição

    for block in ipblocks:

        detail_blocks = block.find_all('div', class_='div_table', recursive=True)[1:]  # Pula a descrição

        for detail_block in detail_blocks:

            key_div = detail_block.find('div', class_='div_table', style='font-weight: bold; display:table;')

            if key_div:
                value_div = key_div.find_next('div', style=lambda
                    value: 'display:table-cell;' in value if value else False)

                if value_div:
                    key_text = clean_html(
                        key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
                    value_text = clean_html(value_div.get_text(strip=True))

                    ipinfo[key_text] = value_text

                    if 'IP Address' in key_text:
                        if ipinfo not in data:
                            data.append(ipinfo)
                            ipinfo = {}

    print(f"OUT {data}")

    if len(data) > 0:
        return data
    else:
        return None


def book_infoReader(address_book_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO BOOK INFO ===========================", 32)

    if DebugMode:
        print(f"{address_book_info}")

    # Qualquer Novo Div Criada Pasta Inserir o Valor
    campos_desejados = ['Symmetric contacts', 'Asymmetric contacts']

    # Lista para armazenar todos os registros
    allRegistros = []

    # Encontrar todos os blocos de mensagem
    contact_blocks = address_book_info.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

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
                    data[remover_espacos_regex(field_name)] = phone_list[1:]

        if len(data) > 0:
            # Adicionar o registro à lista
            if data not in allRegistros:
                allRegistros.append(data)

    if DebugMode:
        # Print dos registros
        for registro in allRegistros:
            print(registro)

    print(f"OUT {allRegistros}")

    if len(allRegistros) > 0:
        return allRegistros
    else:
        return None


def groups_infoReader(groups_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO GROUPS INFO ===========================", 32)

    if DebugMode:
        print(f"{groups_info}")

    # Qualquer Novo Div Criada Pasta Inserir o Valor
    campos_desejados = ['Picture', 'Thumbnail', 'ID', 'Creation', 'Size', 'Description', 'Subject']

    # Lista para armazenar todos os registros
    allRegistros = {}

    ownedRegistros = []
    participatingRegistros = []

    # Encontrar todos os blocos de mensagem
    group_blocks = groups_info.find_all("div", class_="div_table", style="font-weight: bold; display:table;")

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

            if 'Owned' in field_name_text:
                GroupOwned = True
                GroupParticipating = False

            if 'Participating' in field_name_text:
                GroupOwned = False
                GroupParticipating = True

            field_value_div = field.find("div",
                                         style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")

            if field_value_div:
                field_value = field_value_div.text.strip()
                field_name = field_name_text.replace(field_value, '').strip()

                if field_name in campos_desejados:
                    if 'Picture' in field_name:
                        data[remover_espacos_regex(field_name)] = field_value.replace('Linked Media File:', '')
                    else:
                        data[remover_espacos_regex(field_name)] = field_value

                    if 'Subject' in field_name:

                        if GroupOwned and data not in ownedRegistros:
                            ownedRegistros.append(data)

                        if GroupParticipating and data not in participatingRegistros:
                            participatingRegistros.append(data)

                        data = {}

    allRegistros['ownedGroups'] = ownedRegistros
    allRegistros['ParticipatingGroups'] = participatingRegistros

    if DebugMode:
        # Print dos registros
        for registro in allRegistros:
            print(registro)

        for registro in ownedRegistros:
            print(registro)

        for registro in participatingRegistros:
            print(registro)

    print(f"OUT Owned {ownedRegistros}")

    print(f"OUT Participating {participatingRegistros}")

    print(f"OUT All {allRegistros}")

    if len(allRegistros) > 0:
        return allRegistros
    else:
        return None


def ncmec_reportsReader(ncmec_reports, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO NCMEC REPORTS ===========================", 32)

    if DebugMode:
        print(f"{ncmec_reports}")

    data = {}

    fields = ncmec_reports.find_all("div", class_="div_table", style="font-weight: bold;")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()
            data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None


def connection_infoReader(connection_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO CONNECTION INFO ===========================", 32)

    if DebugMode:
        print(f"{connection_info}")

    data = {}

    fields = connection_info.find_all("div", class_="div_table", style="font-weight: bold;")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()

            if 'Device Id' in field_name or 'Service start' in field_name or 'Device Type' in field_name or 'App Version' in field_name or 'Device OS Build Number' in field_name or 'Connection State' in field_name or 'Last seen' in field_name or 'Last IP' in field_name:
                data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None


def web_infoReader(web_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO WEB INFO ===========================", 32)

    if DebugMode:
        print(f"{web_info}")

    data = {}

    fields = web_info.find_all("div", class_="div_table", style="font-weight: bold;")[1:]  # Pula a descrição

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()

            if 'Web Info' in field_name or 'Availability' in field_name or 'Online Since' in field_name or 'Platform' in field_name or 'Version' in field_name:
                data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None


def small_medium_business_infoReader(small_medium_business_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO SMALL MEDIUM BUSINESS ===========================", 32)

    if DebugMode:
        print(f"{small_medium_business_info}")

    data = {}

    fields = small_medium_business_info.find_all("div", class_="div_table", style="font-weight: bold;")[1:]  # Pula a descrição

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()

            if 'Small Medium Business' in field_name or 'Address' in field_name or 'Email' in field_name or 'Name' in field_name:
                data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None


def device_infoReader(device_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO DEVICE INFO ===========================", 32)

    if DebugMode:
        print(f"{device_info}")

    data = {}

    fields = device_info.find_all("div", class_="div_table", style="font-weight: bold;")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", style="font-weight: bold; display:table;")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div",
                                     style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()

            if 'Device Id' in field_name or 'App Version' in field_name or 'OS Version' in field_name or 'OS Build Number' in field_name or 'Device Manufacturer' in field_name or 'Device Mode' in field_name:
                data[remover_espacos_regex(field_name)] = field_value

    print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None
