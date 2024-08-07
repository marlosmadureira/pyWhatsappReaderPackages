# DATA QUANDO COMEÇOU EXECUTAR O NOVO ROBOZINHO ::/::/2024
# APT INSTALL
# sudo apt install python3-psycopg2 python3-dotenv
# pip3 freeze > requirements.txt

# PIP3 INSTALL
# pip3 install markdownify watchdog python-dotenv psycopg2-binary slacker-log-handler beautifulsoup4 lxml memory-profiler zeep 

# Excecutando para Ver o Grafico
# mprof run pyMain.py

# Ver Grafico de Execução
# mprof plot

Cores do Texto
```
Nome	    Código
Preto	        30
Vermelho	    31
Verde	        32
Amarelo	        33
Azul	        34
Magenta	        35
Ciano	        36
Branco	        37
Preto Claro	    90
Vermelho Claro	91
Verde Claro	    92
Amarelo Claro	93
Azul Claro	    94
Magenta Claro	95
Ciano Claro	    96
Branco Claro	97

Cores de Fundo
Nome	            Código
Fundo Preto	        40
Fundo Vermelho	    41
Fundo Verde	        42
Fundo Amarelo	    43
Fundo Azul	        44
Fundo Magenta	    45
Fundo Ciano	        46
Fundo Branco	    47
Fundo Preto Claro	100
Fundo Vermelho Claro101
Fundo Verde Claro	102
Fundo Amarelo Claro	103
Fundo Azul Claro	104
Fundo Magenta Claro	105
Fundo Ciano Claro	106
Fundo Branco Claro	107

Estilos de Texto
Nome	    Código
Reset	    0
Negrito	    1
Fraco	    2
Itálico	    3
Sublinhado	4
Piscando	5
Invertido	7
Oculto	    8
Tachado	    9

def print_colored(text, color_code, style_code=0, bg_color_code=0):
    print(f"\033[{style_code};{color_code};{bg_color_code}m{text}\033[0m")

# Exemplo de uso
print_colored("Texto em Vermelho", 31)
print_colored("Texto em Verde Claro", 92)
print_colored("Texto em Amarelo com Fundo Azul", 33, bg_color_code=44)
print_colored("Texto Negrito e Sublinhado", 37, style_code=1)
```