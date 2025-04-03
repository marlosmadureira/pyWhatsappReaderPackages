import json
import os
import re
import logging
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

# Configuração global do logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class DynamicSentenceParser:
    def __init__(self):
        # Inicialização do logger
        self.logger = logging.getLogger(self.__class__.__name__)

        # Padrões para diferentes tipos de parsing
        self.PARAMETERS_PATTERNS = {
            "Service": r"Service(\w+)",
            "Internal Ticket Number": r"Internal Ticket Number(\d+)",
            "Account Identifier": r"Account Identifier\s*([\+\d\s-]+)",
            "Account Type": r"Account Type(\w+)",
            "User Generated": r"Generated(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
            "Date Range": r"Date Range(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC to \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
            "Ncmec Reports Definition": r"Ncmec Reports Definition(NCMEC Reports: [\w\s\(\)]+)",
            "NCMEC CyberTip Numbers": r"NCMEC CyberTip Numbers([\w\s]+)",
            "Emails Definition": r"Emails Definition(Emails: [\w\s':]+)",
            "Registered Email Addresses": r"Registered Email Addresses([\w\s]+)"
        }

        self.CONNECTION_PATTERNS = {
            "Device Id": r"Device Id(\d+)",
            "Service start": r"Service start(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
            "Device Type": r"Device Type(\w+)",
            "App Version": r"App Version([\d\.]+)",
            "Device OS Build Numberos": r"Device OS Build Number([\w\s:]+)",
            "Connection State": r"Connection State(\w+)",
            "Online Since": r"Online Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
            "Connected from": r"Connected from([\w\.:]+)",
            "Push Name": r"Push Name(.*)"
        }

        self.DEVICE_PATTERNS = {
            "Device Id": r"Device Id(\d+)",
            "App Version": r"App Version([\w\-\.]+)",
            "OS Version": r"OS Version([\w\.]+)",
            "OS Build Number": r"OS Build Number\s*([\w\s]*)",
            "Device Manufacturer": r"Device Manufacturer([\w\s]+)",
            "Device Model": r"Device Model([\w\s]+)"
        }

        self.WEB_PATTERNS = {
            "Version": r"Version([\w\.]+)",
            "Platform": r"Platform([\w\.]+)",
            "Device Manufacturer": r"Device Manufacturer([\w\s]+)",
            "Online Since": r"Online Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)",
            "Inactive Since": r"Inactive Since(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)"
        }

        self.SMALL_PATTERNS = {
            "Small Medium Business": r"Small Medium Business([\w\.]+)",
            "Address": r"Address([\w\.]+)",
            "Email": r"Email([\w\.]+)",
            "Name": r"Name([\w\.]+)",
        }

    def _clean_content(self, content: str) -> str:
        """Limpa e normaliza o conteúdo"""
        content = re.sub(r'\\', '', content.strip())
        return '\n'.join(line for line in content.splitlines() if line.strip())

    def _parse_with_patterns(self, content: str, patterns: Dict[str, str]) -> Optional[Dict]:
        """Método genérico para parsing com padrões"""
        try:
            content = self._clean_content(content)
            results = {}

            for key, pattern in patterns.items():
                match = re.search(pattern, content)
                if match:
                    results[self._remover_espacos_regex(key)] = match.group(1).strip()

            return results if results else None

        except Exception as e:
            self.logger.error(f"Erro ao processar padrões: {str(e)}")
            return None

    def _remover_espacos_regex(self, text: str) -> str:
        """Remove espaços e caracteres especiais do texto"""
        return re.sub(r'\s+', '', text)

    def parse_parameters(self, content: str) -> Optional[Dict]:
        """Parse parameters from content"""
        return self._parse_with_patterns(content, self.PARAMETERS_PATTERNS)

    def parse_connection(self, content: str) -> Optional[Dict]:
        """Parse connection information"""
        return self._parse_with_patterns(content, self.CONNECTION_PATTERNS)

    def parse_device(self, content: str) -> Optional[Dict]:
        """Parse device information"""
        return self._parse_with_patterns(content, self.DEVICE_PATTERNS)

    def parse_web(self, content: str) -> Optional[Dict]:
        """Parse web information"""
        return self._parse_with_patterns(content, self.WEB_PATTERNS)

    def parse_small(self, content: str) -> Optional[Dict]:
        """Parse small business information"""
        return self._parse_with_patterns(content, self.SMALL_PATTERNS)

    def parse_books(self, content: str) -> Optional[List[Dict]]:
        """Parse books/contacts information"""
        try:
            content = self._clean_content(content)

            data = {
                'Symmetriccontacts': [],
                'Asymmetriccontacts': []
            }

            def process_numbers(text):
                if not text:
                    return []
                # Remove páginas de registro e quebras de linha
                text = re.sub(r'WhatsApp Business Record Page \d+', '', text)
                text = ' '.join(text.split())
                # Encontra números com 10-15 dígitos
                numbers = []
                for num in re.finditer(r'\b(\d{10,15})\b', text):
                    numbers.append(num.group(1))
                return sorted(list(set(numbers)))

            # Processa números simétricos
            symmetric_section = re.search(r"Symmetric contacts\s*\d+\s*Total([\s\S]*?)(?=Asymmetric contacts|$)",
                                          content)
            if symmetric_section:
                data['Symmetriccontacts'] = process_numbers(symmetric_section.group(1))

            # Processa números assimétricos
            asymmetric_section = re.search(r"Asymmetric contacts\s*\d+\s*Total([\s\S]*?)(?=Groups|$)", content)
            if asymmetric_section:
                data['Asymmetriccontacts'] = process_numbers(asymmetric_section.group(1))

            if data['Symmetriccontacts'] or data['Asymmetriccontacts']:
                self.logger.info(
                    f"Encontrados {len(data['Symmetriccontacts'])} contatos simétricos e {len(data['Asymmetriccontacts'])} assimétricos")
                return [data]

            return None

        except Exception as e:
            self.logger.error(f"Erro ao processar contatos: {str(e)}")
            return None

    def parse_group_participants(self, content: str) -> Optional[Dict]:
        """Parse group participants information"""
        try:
            content = self._clean_content(content)

            results = {
                "GroupParticipants": [],
                "GroupAdministrators": [],
                "Participants": []
            }

            # Processa cada tipo de participante
            pattern = r"(GroupParticipants|GroupAdministrators|Participants)\s*\d+\s+Total\s+([\d\s]+)"
            matches = re.findall(pattern, content, re.IGNORECASE)

            for key, numbers in matches:
                key = self._remover_espacos_regex(key)
                numbers_text = re.sub(r'WhatsApp Business Record Page \d+', '', numbers)
                cleaned_numbers = re.findall(r'\b\d+\b', numbers_text)
                results[key].extend(cleaned_numbers)

            if any(results.values()):
                self.logger.info(f"Encontrados participantes: {sum(len(v) for v in results.values())} total")
                return results

            return None

        except Exception as e:
            self.logger.error(f"Erro ao processar participantes do grupo: {str(e)}")
            return None

    def process_all_content(self, content: str) -> Dict:
        """Processa todo o conteúdo extraindo todas as informações disponíveis"""
        try:
            resultado = {
                'parameters': self.parse_parameters(content),
                'contacts': self.parse_books(content),
                'ip_addresses': self.parse_ip_addresses(content),
                'connection': self.parse_connection(content),
                'device': self.parse_device(content),
                'web': self.parse_web(content),
                'small': self.parse_small(content),
                'group': self.parse_group(content),
                'group_participants': self.parse_group_participants(content)
            }

            # Remove valores None do resultado
            resultado_filtrado = {
                k: v for k, v in resultado.items() if v is not None
            }

            return resultado_filtrado

        except Exception as e:
            self.logger.error(f"Erro ao processar conteúdo: {str(e)}")
            return {}

    def parse_ip_addresses(self, content: str) -> Optional[List[Dict]]:
        """Parse IP addresses information"""
        try:
            content = self._clean_content(content)

            # Encontra a seção de IPs
            ip_section = re.search(r"Ip Addresses([\s\S]*?)(?=Groups|$)", content)
            if not ip_section:
                return None

            content = ip_section.group(1)
            # Remove páginas de registro
            content = re.sub(r'WhatsApp Business Record Page \d+', '', content)

            # Padrão atualizado para capturar tempo e IP
            pattern = re.compile(r"""
                Time\s*
                (\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)\s*
                IP\s*Address\s*
                ([0-9a-fA-F:\.]+)
            """, re.VERBOSE)

            results = []
            for match in pattern.finditer(content):
                results.append({
                    "Time": match.group(1).strip(),
                    "IPAddress": match.group(2).strip()
                })

            if results:
                self.logger.info(f"Encontrados {len(results)} endereços IP")
                return results

            return None

        except Exception as e:
            self.logger.error(f"Erro ao processar endereços IP: {str(e)}")
            return None

    def read_html_file(self, file_path: str) -> Optional[Dict]:
        """Lê e processa o arquivo HTML"""
        try:
            self.logger.info(f"Iniciando leitura do arquivo: {file_path}")

            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')

                # Remove scripts e estilos
                for element in soup(['script', 'style']):
                    element.decompose()

                content = soup.get_text()
                self.logger.debug(f"Conteúdo HTML extraído. Tamanho: {len(content)}")

                return self.process_all_content(content)

        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo HTML: {str(e)}")
            return None

    def process_all_content(self, content: str) -> Dict:
        """Processa todo o conteúdo extraindo todas as informações disponíveis"""
        resultado = {
            'parameters': self.parse_parameters(content),
            'contacts': self.parse_books(content),
            'ip_addresses': self.parse_ip_addresses(content),
            'connection': self.parse_connection(content),
            'device': self.parse_device(content),
            'web': self.parse_web(content),
            'small': self.parse_small(content),
            'group': self.parse_group(content),
            'group_participants': self.parse_group_participants(content)
        }

        return resultado


def main():
    # Configuração do logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Configurações
    PASTA_ENTRADA = "entrada"
    PASTA_SAIDA = "saida"

    logger = logging.getLogger(__name__)

    # Cria pasta de saída se não existir
    if not os.path.exists(PASTA_SAIDA):
        os.makedirs(PASTA_SAIDA)

    # Inicializa o parser
    parser = DynamicSentenceParser()

    # Processa todos os arquivos HTML na pasta de entrada
    for arquivo in os.listdir(PASTA_ENTRADA):
        if arquivo.endswith('.html'):
            logger.info(f"Processando arquivo: {arquivo}")

            caminho_entrada = os.path.join(PASTA_ENTRADA, arquivo)
            caminho_saida = os.path.join(PASTA_SAIDA, f"{arquivo[:-5]}_resultado.json")

            try:
                # Processa o arquivo
                resultado = parser.read_html_file(caminho_entrada)

                if resultado:
                    # Salva o resultado
                    with open(caminho_saida, 'w', encoding='utf-8') as f:
                        json.dump(resultado, f, ensure_ascii=False, indent=2)
                    logger.info(f"Arquivo processado e salvo em: {caminho_saida}")
                else:
                    logger.warning("Nenhum conteúdo encontrado para processar")

            except Exception as e:
                logger.error(f"Erro ao processar {arquivo}: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()