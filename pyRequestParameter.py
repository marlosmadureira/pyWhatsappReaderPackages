from pyBiblioteca import print_color
def requestReaderParameter(parameters, DebugMode):
    print_color(f"\n=========================== PROCESSANDO REQUEST PARAMENTER ===========================", 32)

    if DebugMode:
        print(f"{parameters}")

    data = {}

    fields = parameters.find_all("div", class_="div_table", style="font-weight: bold;")

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

    if data is not None:
        return data
    else:
        return None