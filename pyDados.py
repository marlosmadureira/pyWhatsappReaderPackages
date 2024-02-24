from pyBiblioteca import print_color


def book_infoReader(address_book_info, DebugMode):
    print_color(f"\n=========================== PROCESSANDO BOOK INFO ===========================", 32)

    if DebugMode:
        print(f"{address_book_info}")


def groups_infoReader(groups_info, DebugMode):
    print_color(f"\n=========================== PROCESSANDO GROUPS INFO ===========================", 32)

    if DebugMode:
        print(f"{groups_info}")


def ncmec_reportsReader(ncmec_reports, DebugMode):
    print_color(f"\n=========================== PROCESSANDO NCMEC REPORTS ===========================", 32)

    if DebugMode:
        print(f"{ncmec_reports}")


def connection_infoReader(connection_info, DebugMode):
    print_color(f"\n=========================== PROCESSANDO CONNECTION INFO ===========================", 32)

    if DebugMode:
        print(f"{connection_info}")


def web_infoReader(web_info, DebugMode):
    print_color(f"\n=========================== PROCESSANDO WEB INFO ===========================", 32)

    if  DebugMode:
        print(f"{web_info}")
