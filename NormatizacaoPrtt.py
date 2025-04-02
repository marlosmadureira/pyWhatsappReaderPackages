import logging
import os
import re
import json
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from dataclasses import dataclass


class MessageParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.PADROES_MENSAGEM = {
            "Timestamp": {
                "pattern": r"Timestamp[\s:]*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
                "tipo": "datetime"
            },
            "Message Id": {
                "pattern": r"Message Id[\s:]*([A-F0-9]{8,32})",  # Entre 8 e 32 caracteres
                "tipo": "string"
            },
            "Sender": {
                "pattern": r"Sender[\s:]*(\d+)",
                "tipo": "int"
            },
            "Recipients": {
                "pattern": r"Recipients[\s:]*(\d+)",
                "tipo": "int"
            },
            "Sender Ip": {
                # Padrão atualizado para capturar IPv4 e IPv6
                "pattern": r"Sender Ip[\s:]*([0-9a-fA-F:.]{7,45})",
                "tipo": "string"
            },
            "Sender Port": {
                "pattern": r"Sender Port[\s:]*(\d+)",
                "tipo": "int"
            },
            "Sender Device": {
                # Padrão atualizado para capturar "smba"
                "pattern": r"Sender Device[\s:]*(\w+)(?=\s+Type|\s*$)",
                "tipo": "string"
            },
            "Type": {
                # Padrão atualizado para capturar "text"
                "pattern": r"Type[\s:]*(\w+)(?=\s+Message Style|\s*$)",
                "tipo": "string"
            },
            "Message Style": {
                # Padrão atualizado para capturar "individual"
                "pattern": r"Message Style[\s:]*(\w+)(?=\s+Message Size|\s*$)",
                "tipo": "string"
            },
            "Message Size": {
                "pattern": r"Message Size[\s:]*(\d+)",
                "tipo": "int"
            }
        }

    def processar_mensagens(self, content: str) -> Optional[List[Dict]]:
        try:
            self.logger.debug(f"Iniciando processamento de mensagens. Tamanho do conteúdo: {len(content)}")

            # Normaliza o conteúdo
            content = re.sub(r'\\', '', content.strip())
            content = '\n'.join(line for line in content.splitlines() if line.strip())

            # Padrão mais flexível para encontrar mensagens
            padrao_mensagem_completa = re.compile(
                r'Timestamp[^}]*?Message Size\s*\d+',
                re.DOTALL | re.IGNORECASE
            )

            # Encontra todas as mensagens
            mensagens = padrao_mensagem_completa.findall(content)
            self.logger.debug(f"Encontradas {len(mensagens)} mensagens brutas")

            if not mensagens:
                self.logger.warning("Nenhuma mensagem encontrada no conteúdo")
                # Debug: mostra os primeiros caracteres do conteúdo
                self.logger.debug(f"Primeiros 200 caracteres do conteúdo:\n{content[:200]}")
                return None

            resultados = []
            for i, mensagem in enumerate(mensagens, 1):
                self.logger.debug(f"Processando mensagem {i}/{len(mensagens)}")
                resultado = {}

                for campo, config in self.PADROES_MENSAGEM.items():
                    match = re.search(config["pattern"], mensagem, re.IGNORECASE)
                    if match:
                        valor = match.group(1).strip()
                        if config["tipo"] == "int":
                            try:
                                valor = int(valor)
                            except ValueError:
                                self.logger.warning(f"Erro ao converter valor para int: {valor}")
                                continue
                        campo_normalizado = campo.lower().replace(' ', '_')
                        resultado[campo_normalizado] = valor
                    else:
                        self.logger.debug(f"Campo não encontrado: {campo}")

                if resultado:
                    # Verifica se tem os campos mínimos necessários
                    campos_obrigatorios = ['timestamp', 'message_id']
                    if all(campo in resultado for campo in campos_obrigatorios):
                        resultados.append(resultado)
                    else:
                        self.logger.warning(f"Mensagem {i} descartada por falta de campos obrigatórios")

            self.logger.info(f"Total de mensagens processadas com sucesso: {len(resultados)}")
            return resultados if resultados else None

        except Exception as e:
            self.logger.error(f"Erro ao processar mensagens: {str(e)}", exc_info=True)
            return None


class CallParser:
    def __init__(self):
        self.PATTERNS = {
            'call': re.compile(
                r'Call Id\s*([\w\d]+)\s*'
                r'Call Creator\s*([\d]+)\s*'
                r'(Events.*?)(?=Call Id|$)',
                re.DOTALL
            ),
            'event': re.compile(
                r'Type\s*(\w+)\s*'
                r'Timestamp\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)\s*'
                r'(?:From\s*([\d]*)\s*To\s*([\d]*)\s*)?'
                r'(?:From Ip\s*([\d\.:a-fA-F]+)\s*From Port\s*(\d+))?'
                r'(?:\s*Media Type\s*([\w]+))?'
            ),
            'participant': re.compile(
                r'Phone Number\s*([\d]+)\s*'
                r'State\s*(\w+)\s*'
                r'Platform\s*(\w+)'
            )
        }

    def parse_calls(self, content: str) -> Optional[List[Dict]]:
        try:
            # Normaliza o conteúdo
            content = re.sub(r'\\', '', content.strip())
            content = '\n'.join(line for line in content.splitlines() if line.strip())

            # Encontra todas as chamadas
            calls = self.PATTERNS['call'].finditer(content)
            results = []

            for call in calls:
                call_data = {
                    "call_id": call.group(1).strip(),
                    "call_creator": call.group(2).strip(),
                    "events": []
                }

                events_section = call.group(3)
                events = self.PATTERNS['event'].finditer(events_section)

                for event in events:
                    event_data = {
                        "type": event.group(1).strip(),
                        "timestamp": event.group(2).strip(),
                        "from": event.group(3).strip() if event.group(3) else '',
                        "to": event.group(4).strip() if event.group(4) else '',
                        "from_ip": event.group(5).strip() if event.group(5) else '',
                        "from_port": event.group(6).strip() if event.group(6) else '',
                        "participants": []
                    }

                    if event.group(7):
                        event_data["media_type"] = event.group(7).strip()

                    # Processa participantes
                    participants = self.PATTERNS['participant'].finditer(events_section)
                    seen_participants = set()

                    for participant in participants:
                        participant_key = (
                            participant.group(1).strip(),
                            participant.group(2).strip(),
                            participant.group(3).strip()
                        )

                        if participant_key not in seen_participants:
                            participant_data = {
                                "phone_number": participant_key[0],
                                "state": participant_key[1],
                                "platform": participant_key[2]
                            }
                            event_data["participants"].append(participant_data)
                            seen_participants.add(participant_key)

                    call_data["events"].append(event_data)

                results.append(call_data)

            return results if results else None

        except Exception as e:
            logging.error(f"Erro ao processar chamadas: {str(e)}")
            return None


class ProcessadorHTML:
    def __init__(self):
        self.message_parser = MessageParser()
        self.call_parser = CallParser()
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def ler_arquivo_html(self, caminho_arquivo: str) -> str:
        """Lê o arquivo HTML e retorna seu conteúdo"""
        try:
            with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                conteudo = arquivo.read()
                self.logger.info(f"Arquivo lido com sucesso. Tamanho: {len(conteudo)} caracteres")
                return conteudo
        except UnicodeDecodeError:
            self.logger.warning("Tentando ler com encoding latin-1")
            with open(caminho_arquivo, 'r', encoding='latin-1') as arquivo:
                return arquivo.read()

    def extrair_conteudo_html(self, html_content: str) -> Dict[str, str]:
        """Extrai o conteúdo relevante do HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        self.logger.debug(f"HTML parseado com BeautifulSoup")

        # Remove scripts e estilos
        for elemento in soup(['script', 'style']):
            elemento.decompose()

        conteudo = {
            'mensagens': '',
            'chamadas': ''
        }

        # Obtém todo o texto
        texto_completo = soup.get_text(separator='\n')

        # Procura por mensagens (padrão mais flexível)
        padrao_mensagem = re.compile(
            r'(Timestamp[^}]*?Message Size\s*\d+)',
            re.DOTALL | re.IGNORECASE
        )
        matches_mensagem = padrao_mensagem.findall(texto_completo)
        if matches_mensagem:
            self.logger.info(f"Encontradas {len(matches_mensagem)} mensagens")
            conteudo['mensagens'] = '\n'.join(matches_mensagem)
            # Debug: mostra exemplo de mensagem encontrada
            self.logger.debug(f"Exemplo de mensagem encontrada:\n{matches_mensagem[0][:200]}...")
        else:
            self.logger.warning("Nenhuma mensagem encontrada")
            # Debug: mostra parte do texto para verificar o formato
            self.logger.debug(f"Amostra do texto completo:\n{texto_completo[:500]}...")

        # Procura por chamadas (mantém o mesmo padrão que já está funcionando)
        padrao_chamada = re.compile(r'Call Id.*?(?=Call Id|$)', re.DOTALL)
        matches_chamada = padrao_chamada.findall(texto_completo)
        if matches_chamada:
            self.logger.info(f"Encontradas {len(matches_chamada)} chamadas")
            conteudo['chamadas'] = '\n'.join(matches_chamada)
        else:
            self.logger.warning("Nenhuma chamada encontrada")

        return conteudo

    def processar_arquivo(self, caminho_arquivo: str) -> Dict:
        """Processa o arquivo HTML e retorna os dados estruturados"""
        self.logger.info(f"Iniciando processamento do arquivo: {caminho_arquivo}")

        # Lê o arquivo
        conteudo_html = self.ler_arquivo_html(caminho_arquivo)

        # Extrai o conteúdo relevante
        conteudo = self.extrair_conteudo_html(conteudo_html)

        # Processa mensagens e chamadas
        resultado = {
            'mensagens': None,
            'chamadas': None
        }

        if conteudo['mensagens']:
            self.logger.info("Processando mensagens encontradas")
            resultado['mensagens'] = self.message_parser.processar_mensagens(conteudo['mensagens'])
            self.logger.info(f"Mensagens processadas: {resultado['mensagens'] is not None}")

        if conteudo['chamadas']:
            self.logger.info("Processando chamadas encontradas")
            resultado['chamadas'] = self.call_parser.parse_calls(conteudo['chamadas'])
            self.logger.info(f"Chamadas processadas: {resultado['chamadas'] is not None}")

        return resultado


def main():
    # Configurações
    PASTA_ENTRADA = "entrada"
    PASTA_SAIDA = "saida"

    logger = logging.getLogger(__name__)

    # Cria pasta de saída se não existir
    if not os.path.exists(PASTA_SAIDA):
        os.makedirs(PASTA_SAIDA)

    processador = ProcessadorHTML()

    # Processa todos os arquivos HTML na pasta de entrada
    for arquivo in os.listdir(PASTA_ENTRADA):
        if arquivo.endswith('.html'):
            logger.info(f"Processando arquivo: {arquivo}")

            caminho_entrada = os.path.join(PASTA_ENTRADA, arquivo)
            caminho_saida = os.path.join(PASTA_SAIDA, f"{arquivo[:-5]}_resultado.json")

            try:
                # Processa o arquivo
                resultado = processador.processar_arquivo(caminho_entrada)

                # Verifica se encontrou algum conteúdo
                if resultado['mensagens'] is None and resultado['chamadas'] is None:
                    logger.warning("Nenhum conteúdo foi encontrado no arquivo")

                # Salva o resultado
                with open(caminho_saida, 'w', encoding='utf-8') as f:
                    json.dump(resultado, f, ensure_ascii=False, indent=2)

                logger.info(f"Arquivo processado e salvo em: {caminho_saida}")

            except Exception as e:
                logger.error(f"Erro ao processar {arquivo}: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()