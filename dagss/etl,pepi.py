import datetime
import json 
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
@dag(
    start_date=datetime.datetime(2024, 8, 31),
    schedule="@daily", 
    catchup=False
)
def etl_pipeline():

    @task
    def extract_data():
        file_paths = [
            '/dados/COTAHIST_A2022.TXT',
            '/dados/COTAHIST_A2023.TXT',
            '/dados/COTAHIST_A2024.TXT',
            '/dados/atualizacao_diaria_b3.TXT'  # Arquivo .TXT para a atualização diária
        ]
        
        data = []
        for path in file_paths:
            with open(path, 'r') as file:
                data.extend(file.readlines())  # Adiciona os dados lidos ao conjunto de dados

        return data

    @task
    def transform_data(raw_data):
        transformed_data = []
        for line in raw_data:
            acao = line[12:16].strip()
            preco_abertura = float(line[56:69].strip())
            preco_fechamento = float(line[108:121].strip())
            quantidade = int(line[170:188].strip())
            data = datetime.datetime.strptime(line[2:10], "%Y%m%d")

            transformed_data.append({
                "acao": acao,
                "preco_abertura": preco_abertura,
                "preco_fechamento": preco_fechamento,
                "quantidade": quantidade,
                "data": data
            })

        with open('/dados/transformed_data.json', 'w') as json_file:
            json.dump(transformed_data, json_file, default=str, indent=4)

        return transformed_data

    @task
    def load_data(transformed_data):
        mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        for record in transformed_data:
            # Carga de dados históricos
            cursor.execute("INSERT INTO Dim_Acao (codigo_acao) VALUES (%s)", (record["acao"],))
            cursor.execute("INSERT INTO Dim_Calendario (data, ano, trimestre, mes, dia) VALUES (%s, %s, %s, %s, %s)", 
                           (record["data"], record["data"].year, (record["data"].month-1)//3 + 1, record["data"].month, record["data"].day))
            cursor.execute("INSERT INTO Fato_Cotacoes (preco_abertura, preco_fechamento, quantidade) VALUES (%s, %s, %s)", 
                           (record["preco_abertura"], record["preco_fechamento"], record["quantidade"]))

        conn.commit()
        cursor.close()
        conn.close()

    @task
    def load_daily_update(transformed_data):
        mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        for record in transformed_data:
            # Atualização das cotações diárias
            cursor.execute(""" 
                UPDATE Fato_Cotacoes
                SET preco_abertura = %s, preco_fechamento = %s, quantidade = %s
                WHERE codigo_acao = %s AND data = %s
            """, (record["preco_abertura"], record["preco_fechamento"], record["quantidade"], record["acao"], record["data"]))

        conn.commit()
        cursor.close()
        conn.close()

    # Definindo a sequência de execução das tarefas
    raw_data = extract_data()  
    transformed_data = transform_data(raw_data) 
    load_data(transformed_data)
    load_daily_update(transformed_data)

    # Definindo a sequência de execução
    extract_data() >> transform_data() >> load_data() >> load_daily_update()
    
etl_pipeline()
