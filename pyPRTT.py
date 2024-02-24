def message_logReader(message_log, DebugMode):
    if DebugMode:
        print(message_log)

    data = {}

    fields = message_log.find_all("div", class_="div_table", style="font-weight: bold;")

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
            data[field_name] = field_value

    print(data)

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