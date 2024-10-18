# -*- coding: utf-8 -*-
# !/usr/bin/python3
import os
import json
import psycopg2

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def print_color(text, color_code):
    print(f"\033[{color_code}m{text}\033[0m")

def conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS):
    con = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return con

def manutencao():
    # JSON original
    json_str = "[{'Symmetriccontacts': ['5517988104766', '5517988293549', '5517988436071', '5517988473206', '5517991464641', '5517991506441', '5517991522966', '5517991977678', '5517992054790', '5517992175506', '5517992236182', '5517992355939', '5517992829053', '5517992840833', '5517996125606', '5517996689247', '5517997122598'], 'Asymmetriccontacts': ['551732792200', '556292022443', '558287107733', '5516991060684', '5517981016634', '5517981031938', '5517981350049', '5517981365583', '5517981467021', '5517981543476', '5517981593340', '5517981601155', '5517981644193', '5517988007413', '5517988026272', '5517988100548', '5517988124007', '5517988216872', '5517988335440', '5517988350258', '5517988360466', '5517991209676', '5517991217530', '5517991347343', '5517991644149', '5517991662545', '5517991686050', '5517991701777', '5517991721050', '5517991760990', '5517991940530', '5517992062434', '5517992065997', '5517992069244', '5517992080551', '5517992087840', '5517992095262', '5517992097458', '5517992135375', '5517992139932', '5517992216785', '5517992244504', '5517992276962', '5517992340902', '5517992354598', '5517992401954', '5517992536065', '5517992576722', '5517992591290', '5517992596797', '5517992626215', '5517992631947', '5517992635202', '5517992643297', '5517992667263', '5517992676015', '5517992757323', '5517992765558', '5517996026857', '5517996201537', '5517996289946', '5517996438027', '5517996576403', '5517996582246', '5517996665575', '5517997138854', '5517997357577', '5517997628603', '5517997677809', '5517997761821', '5517997812458']}]"

    # Corrigir a string JSON
    json_str = json_str.replace("'", '"')  # Trocar aspas simples por duplas

    # Carregar o JSON
    data = json.loads(json_str)

    # Acessar os contatos assimétricos
    asymmetric_contacts = data[0]['Asymmetriccontacts']

    AccountIdentifier = f"5517988380147"

    with conectBD(DB_HOST, DB_NAME, DB_USER, DB_PASS) as con:
        db = con.cursor()
        # Percorrer e exibir os contatos assimétricos um por um
        for contact in asymmetric_contacts:
            sqlInsert = f"INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES (%s, %s, %s, %s, %s)"

            try:
                db.execute(sqlInsert, (contact, 'A', AccountIdentifier, 135831, 18334))
                print_color(f"{db.query}", 32)
                con.commit()
            except Exception as e:
                print_color(f"{db.query} {e}", 31)
                db.execute("rollback")
                pass

    db.close()
    con.close()

if __name__ == '__main__':
    manutencao()