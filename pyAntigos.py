#REQUEST

# def requestReaderParameter(parameters, DebugMode, Out):
#     if DebugMode:
#         print(f"{parameters}")
#
#     data = {}
#
#     tipoHtml(parameters)
#
#     fields = parameters.find_all("div", class_="div_table outer")
#
#     for field in fields:
#         # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#         field_name_div = field.find("div", class_="div_table inner")
#         field_name_text = field_name_div.text.strip() if field_name_div else ""
#         # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#         field_value_div = field.find("div", class_="most_inner")
#         if field_value_div:
#             field_value = field_value_div.text.strip()
#             # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#             field_name = field_name_text.replace(field_value, '').strip()
#             data[remover_espacos_regex(field_name)] = field_value
#
#     if Out:
#         print_color(f"\n=========================== PROCESSANDO REQUEST PARAMENTER ===========================", 32)
#         print(f"OUT {data}")
#
#     if data is not None:
#         return data
#     else:
#         return None

# def requestReaderParameter(parameters, DebugMode, Out):
#     if DebugMode:
#         print(f"{parameters}")
#
#     data = {}
#
#     fields = parameters.find_all("div", class_="div_table", style="font-weight: bold;")
#
#     for field in fields:
#         # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#         field_name_div = field.find("div", style="font-weight: bold; display:table;")
#         field_name_text = field_name_div.text.strip() if field_name_div else ""
#         # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#         field_value_div = field.find("div",
#                                      style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#         if field_value_div:
#             field_value = field_value_div.text.strip()
#             # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#             field_name = field_name_text.replace(field_value, '').strip()
#             data[remover_espacos_regex(field_name)] = field_value
#
#     if Out:
#         print_color(f"\n=========================== PROCESSANDO REQUEST PARAMENTER ===========================", 32)
#         print(f"OUT {data}")
#
#     if data is not None:
#         return data
#     else:
#         return None

# DADOS

# def emails_infoReader(emails_info, fileName, DebugMode, Out):
#     if emails_info is not None:
#
#         if DebugMode:
#             print(f"{emails_info}")
#
#         data = {}
#
#         fields = emails_info.find_all("div", class_="div_table", style="font-weight: bold;")
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#                 data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO EMAIL ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# def ip_addresses_infoReader(ip_addresses_info, fileName, DebugMode, Out):
#     if ip_addresses_info is not None:
#
#         if DebugMode:
#             print(f"{ip_addresses_info}")
#
#         data = []
#         ipinfo = {}
#
#         ipblocks = ip_addresses_info.find_all('div', class_='div_table')[1:]  # Pula a descrição
#
#         for block in ipblocks:
#
#             detail_blocks = block.find_all('div', class_='div_table', recursive=True)[1:]  # Pula a descrição
#
#             for detail_block in detail_blocks:
#
#                 key_div = detail_block.find('div', class_='div_table', style='font-weight: bold; display:table;')
#
#                 if key_div:
#                     value_div = key_div.find_next('div', style=lambda
#                         value: 'display:table-cell;' in value if value else False)
#
#                     if value_div:
#                         key_text = clean_html(
#                             key_div.get_text(strip=True).replace(value_div.get_text(strip=True), '').strip())
#                         value_text = clean_html(value_div.get_text(strip=True))
#
#                         ipinfo[remover_espacos_regex(key_text)] = value_text
#
#                         if 'IP Address' in key_text:
#                             if ipinfo not in data:
#                                 data.append(ipinfo)
#                                 ipinfo = {}
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO IP ADDRESS ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if len(data) > 0:
#             return data
#         else:
#             return None
#     else:
#         return None

# def book_infoReader(address_book_info, fileName, DebugMode, Out):
#     if address_book_info is not None:
#
#         if DebugMode:
#             print(f"{address_book_info}")
#
#         # Qualquer Novo Div Criada Pasta Inserir o Valor
#         campos_desejados = ['Symmetric contacts', 'Asymmetric contacts']
#
#         # Lista para armazenar todos os registros
#         allRegistros = []
#
#         # Encontrar todos os blocos de mensagem
#         contact_blocks = address_book_info.find_all("div", class_="div_table",
#                                                     style="font-weight: bold; display:table;")
#
#         # Iterar sobre cada bloco de mensagem
#         for block in contact_blocks:
#             # Dicionário para armazenar os dados de um registro
#             data = {}
#
#             # Encontrar todos os campos dentro de um bloco
#             fields = block.find_all("div", class_="div_table", style="font-weight: bold;")
#
#             # Iterar sobre cada campo e extrair informações
#             for field in fields:
#                 field_name_div = field.find("div", style="font-weight: bold; display:table;")
#                 field_name_text = field_name_div.text.strip() if field_name_div else ""
#
#                 field_value_div = field.find("div",
#                                              style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#
#                 # Obter o texto dentro da tag <div>
#                 phone_text = field_value_div.get_text(separator='\n')
#                 # Dividir o texto em uma lista usando quebras de linha
#                 phone_list = phone_text.split('\n')
#
#                 if field_value_div:
#                     field_value = field_value_div.text.strip()
#                     field_name = field_name_text.replace(field_value, '').strip()
#
#                     if field_name in campos_desejados:
#                         data[remover_espacos_regex(field_name)] = phone_list[1:]
#
#             if len(data) > 0:
#                 # Adicionar o registro à lista
#                 if data not in allRegistros:
#                     allRegistros.append(data)
#
#         if DebugMode:
#             # Print dos registros
#             for registro in allRegistros:
#                 print(registro)
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO BOOK INFO ===========================", 32)
#
#             print(f"OUT {allRegistros}")
#
#         if len(allRegistros) > 0:
#             return allRegistros
#         else:
#             return None
#     else:
#         return None

# def groups_infoReader(groups_info, fileName, DebugMode, Out):
#     if groups_info is not None:
#
#         if DebugMode:
#             print(f"{groups_info}")
#
#         # Qualquer Novo Div Criada Pasta Inserir o Valor
#         campos_desejados = ['Picture', 'Thumbnail', 'ID', 'Creation', 'Size', 'Description', 'Subject']
#
#         # Lista para armazenar todos os registros
#         allRegistros = {}
#
#         ownedRegistros = []
#         participatingRegistros = []
#
#         # Encontrar todos os blocos de mensagem
#         group_blocks = groups_info.find_all("div", class_="div_table", style="font-weight: bold; display:table;")
#
#         # Iterar sobre cada bloco de mensagem
#         for block in group_blocks:
#             # Dicionário para armazenar os dados de um registro
#             data = {}
#
#             # Encontrar todos os campos dentro de um bloco
#             fields = block.find_all("div", class_="div_table", style="font-weight: bold;")
#
#             # Iterar sobre cada campo e extrair informações
#             for field in fields:
#                 field_name_div = field.find("div", style="font-weight: bold; display:table;")
#                 field_name_text = field_name_div.text.strip() if field_name_div else ""
#
#                 if 'Owned' in field_name_text:
#                     GroupOwned = True
#                     GroupParticipating = False
#
#                 if 'Participating' in field_name_text:
#                     GroupOwned = False
#                     GroupParticipating = True
#
#                 field_value_div = field.find("div",
#                                              style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#
#                 if field_value_div:
#                     field_value = field_value_div.text.strip()
#                     field_name = field_name_text.replace(field_value, '').strip()
#
#                     if field_name in campos_desejados:
#                         if 'Picture' in field_name:
#                             data[remover_espacos_regex(field_name)] = field_value.replace('Linked Media File:', '')
#                         else:
#                             data[remover_espacos_regex(field_name)] = field_value
#
#                         if 'Subject' in field_name:
#
#                             if GroupOwned and data not in ownedRegistros:
#                                 ownedRegistros.append(data)
#
#                             if GroupParticipating and data not in participatingRegistros:
#                                 participatingRegistros.append(data)
#
#                             data = {}
#
#         allRegistros['ownedGroups'] = ownedRegistros
#         allRegistros['ParticipatingGroups'] = participatingRegistros
#
#         if DebugMode:
#             # Print dos registros
#             for registro in allRegistros:
#                 print(registro)
#
#             for registro in ownedRegistros:
#                 print(registro)
#
#             for registro in participatingRegistros:
#                 print(registro)
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO GROUPS INFO ===========================", 32)
#
#             # print(f"OUT Owned {ownedRegistros}")
#             #
#             # print(f"OUT Participating {participatingRegistros}")
#
#             print(f"OUT {allRegistros}")
#
#         if len(allRegistros) > 0:
#             return allRegistros
#         else:
#             return None
#     else:
#         return None

# def ncmec_reportsReader(ncmec_reports, fileName, DebugMode, Out):  # SEM AMOSTRA PARA TESTAR
#     if ncmec_reports is not None:
#
#         if DebugMode:
#             print(f"{ncmec_reports}")
#
#         data = {}
#
#         fields = ncmec_reports.find_all("div", class_="div_table", style="font-weight: bold;")
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#                 data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO NCMEC REPORTS ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# def connection_infoReader(connection_info, fileName, DebugMode, Out):  # SEM AMOSTRA PARA TESTAR
#     if connection_info is not None:
#
#         if DebugMode:
#             print(f"{connection_info}")
#
#         data = {}
#
#         fields = connection_info.find_all("div", class_="div_table", style="font-weight: bold;")
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#
#                 if 'Device Id' in field_name or 'Service start' in field_name or 'Device Type' in field_name or 'App Version' in field_name or 'Device OS Build Number' in field_name or 'Connection State' in field_name or 'Last seen' in field_name or 'Last IP' in field_name:
#                     data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO CONNECTION INFO ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# def web_infoReader(web_info, fileName, DebugMode, Out):  # SEM AMOSTRA PARA TESTAR
#     if web_info is not None:
#
#         if DebugMode:
#             print(f"{web_info}")
#
#         data = {}
#
#         fields = web_info.find_all("div", class_="div_table", style="font-weight: bold;")[1:]  # Pula a descrição
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#
#                 if 'Web Info' in field_name or 'Availability' in field_name or 'Online Since' in field_name or 'Platform' in field_name or 'Version' in field_name:
#                     data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO WEB INFO ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# def small_medium_business_infoReader(small_medium_business_info, fileName, DebugMode, Out):  # SEM AMOSTRA PARA TESTAR
#     if small_medium_business_info is not None:
#
#         if DebugMode:
#             print(f"{small_medium_business_info}")
#
#         data = {}
#
#         fields = small_medium_business_info.find_all("div", class_="div_table", style="font-weight: bold;")[
#                  1:]  # Pula a descrição
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#
#                 if 'Small Medium Business' in field_name or 'Address' in field_name or 'Email' in field_name or 'Name' in field_name:
#                     data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO SMALL MEDIUM BUSINESS ===========================",
#                         32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# def device_infoReader(device_info, fileName, DebugMode, Out):
#     if device_info is not None:
#
#         if DebugMode:
#             print(f"{device_info}")
#
#         data = {}
#
#         fields = device_info.find_all("div", class_="div_table", style="font-weight: bold;")
#
#         for field in fields:
#             # Tenta encontrar o nome do campo de uma maneira que exclua o valor
#             field_name_div = field.find("div", style="font-weight: bold; display:table;")
#             field_name_text = field_name_div.text.strip() if field_name_div else ""
#             # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
#             field_value_div = field.find("div",
#                                          style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#             if field_value_div:
#                 field_value = field_value_div.text.strip()
#                 # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
#                 field_name = field_name_text.replace(field_value, '').strip()
#
#                 if 'Device Id' in field_name or 'App Version' in field_name or 'OS Version' in field_name or 'OS Build Number' in field_name or 'Device Manufacturer' in field_name or 'Device Mode' in field_name:
#                     data[remover_espacos_regex(field_name)] = field_value
#
#         if Out:
#             print_color(f"\n=========================== PROCESSANDO DEVICE INFO ===========================", 32)
#
#             print(f"OUT {data}")
#
#         if data is not None:
#             return data
#         else:
#             return None
#     else:
#         return None

# PRTT

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