import mysql.connector

def extrair_e_inserir(arquivos_txt):
    conn = mysql.connector.connect(
        host="localhost",
        user="danilo",
        password="d@n1l0bs2003",
        database="b3_etl"
    )
    cursor = conn.cursor()

    # Processar cada arquivo
    for arquivo_txt in arquivos_txt:
        with open(arquivo_txt, 'r') as file:
            for linha in file:
                codigo_acao = linha[0:6].strip()
                nome_acao = linha[6:19].strip()
                nome_intermediaria = linha[22:60].strip()
                nome_empresa = linha[90:120].strip()
                preco_abertura = float(linha[60:70].strip())
                preco_fechamento = float(linha[70:80].strip())
                quantidade = int(linha[80:90].strip())
                data = linha[120:130].strip()

                # Inserir dados no banco
                cursor.execute("""
                    INSERT INTO Dim_Acao (codigo_acao, nome_acao) VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE nome_acao = %s
                """, (codigo_acao, nome_acao, nome_acao))

                cursor.execute("""
                    INSERT INTO Dim_Intermediaria (nome_intermediaria) VALUES (%s)
                    ON DUPLICATE KEY UPDATE nome_intermediaria = %s
                """, (nome_intermediaria, nome_intermediaria))

                cursor.execute("""
                    INSERT INTO Dim_Empresa (nome_empresa) VALUES (%s)
                    ON DUPLICATE KEY UPDATE nome_empresa = %s
                """, (nome_empresa, nome_empresa))

                ano, mes = int(data[:4]), int(data[4:6])

                cursor.execute("""
                    INSERT INTO Dim_Calendario (ano, mes) VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE ano = %s, mes = %s
                """, (ano, mes, ano, mes))

                # Inserir na tabela Fato_Cotacoes
                cursor.execute("""
                    INSERT INTO Fato_Cotacoes (codigo_acao, preco_abertura, preco_fechamento, quantidade)
                    VALUES (%s, %s, %s, %s)
                """, (codigo_acao, preco_abertura, preco_fechamento, quantidade))

    conn.commit() 
    cursor.close()
    conn.close()

# Lista de arquivos a serem processados
arquivos_txt = ['dados/COTAHIST_A2022.txt', 'dados/COTAHIST_A2023.txt', 'dados/COTAHIST_A2024.txt']
extrair_e_inserir(arquivos_txt)
