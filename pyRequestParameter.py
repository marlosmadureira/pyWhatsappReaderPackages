from pyBiblioteca import print_color, remover_espacos_regex


def requestReaderParameter(parameters, DebugMode, Out, tag1, tag2, tag3, tag4):
    if DebugMode:
        print(f"{parameters}")

    data = {}

    fields = parameters.find_all("div", class_=f"{tag1}")

    for field in fields:
        # Tenta encontrar o nome do campo de uma maneira que exclua o valor
        field_name_div = field.find("div", class_=f"{tag2}")
        field_name_text = field_name_div.text.strip() if field_name_div else ""
        # Se houver um valor associado diretamente, vamos removê-lo do nome do campo
        field_value_div = field.find("div", class_=f"{tag3}")
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