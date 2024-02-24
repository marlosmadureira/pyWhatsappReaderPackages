def message_logReader(message_log, DebugMode):
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
            todos_os_registros.append(data)

    # Print dos registros
    for registro in todos_os_registros:
        print(registro)

    if data is not None:
        return data
    else:
        return None


def call_logsReader(call_logs, DebugMode):
    if DebugMode:
        print(call_logs)


# # Structure to store the extracted data
# data = []
#
# # Find all blocks representing a message
# message_blocks = soup.find_all("div", class_="div_table", style="font-weight: bold; display:table;")
#
# for block in message_blocks:
#     # Check if the nested structure is present
#     nested_structure = block.find("div", class_="div_table", style="font-weight: bold; display:table;")
#
#     if nested_structure:
#         # Dictionary to store the data of a message
#         message_data = {}
#
#         # Find all fields within the message block
#         fields = nested_structure.find_all("div", style="font-weight: bold; display:table;")
#
#         for field in fields:
#             field_name = field.text.strip()
#
#             # Find the next sibling element with the value of the field
#             value_block = field.find_next("div", style="font-weight: normal; display:table-cell; padding: 2px; word-break: break-word; word-wrap: break-word !important;")
#
#             if value_block:
#                 field_value = value_block.text.strip()
#                 message_data[field_name] = field_value
#
#         # Add the data of the current message to the list
#         data.append(message_data)
#
# # Print the extracted data
# for message in data:
#     print("\n".join([f"{key}{value}" for key, value in message.items()]))