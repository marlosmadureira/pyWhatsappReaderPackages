from pyBiblioteca import print_color, remover_espacos_regex


def requestReaderParameter(parameters, DebugMode, Out):
    if DebugMode:
        print(f"{parameters}")

    data = {}

    if 'display:table-cell' in str(parameters):
        print_color(f"\nPENULTIMO PADRÃO HTML display:table-cell", 36)
    elif 'div_table outer' in str(parameters):
        print_color(f"\nULTIMO PADRÃO HTML div_table outer", 36)
    else:
        print_color(f"\nPADRÃO NÃO IDENTIFICADO", 36)

    fields = parameters.find_all("div", class_="div_table outer")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", class_="div_table inner")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div", class_="most_inner")
        if field_value_div:
            field_value = field_value_div.text.strip()
            # Supondo que o valor sempre segue o nome do campo na mesma linha, podemos substituir o valor por '' para obter apenas o nome do campo
            field_name = field_name_text.replace(field_value, '').strip()
            data[remover_espacos_regex(field_name)] = field_value

    if Out:
        print_color(f"\n=========================== PROCESSANDO REQUEST PARAMENTER ===========================", 32)
        print(f"OUT {data}")

    if data is not None:
        return data
    else:
        return None

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
