from pyBiblioteca import print_color, grava_log, remover_espacos_regex


def book_infoReader(address_book_info, fileName, DebugMode):
    print_color(f"\n=========================== PROCESSANDO BOOK INFO ===========================", 32)

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

                    # grava_log(remover_espacos_regex(field_name), 'LogKeyContatct.txt')

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

                    # grava_log(remover_espacos_regex(field_name), 'LogKeyGroup.txt')

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

    if allRegistros is not None:
        return allRegistros
    else:
        return None


def ncmec_reportsReader(ncmec_reports, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO NCMEC REPORTS ===========================", 32)

    return None


def connection_infoReader(connection_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO CONNECTION INFO ===========================", 32)

    return None


def web_infoReader(web_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO WEB INFO ===========================", 32)

    return None


def ip_infoReader(ip_info, fileName, DebugMode):  # SEM AMOSTRA PARA TESTAR
    print_color(f"\n=========================== PROCESSANDO IP INFO ===========================", 32)

    return None
