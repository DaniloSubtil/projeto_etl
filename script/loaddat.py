from airflow.providers.mysql.hooks.mysql import MySqlHook

def load_data_to_mysql(transformed_data):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    for record in transformed_data:
        query = """INSERT INTO cotacoes (codigo_acao, preco, ano, mes, dia, trimestre, semestre, dia_da_semana, nome_mes)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, record)

    conn.commit()
    cursor.close()
    conn.close()
