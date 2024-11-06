import datetime

def transforma_calendario(data):
    data_formatada = datetime.datetime.strptime(data, '%Y-%m-%d')
    ano = data_formatada.year
    mes = data_formatada.month
    dia = data_formatada.day
    trimestre = (mes - 1) // 3 + 1
    semestre = (mes - 1) // 6 + 1
    dia_da_semana = data_formatada.strftime('%A')
    nome_mes = data_formatada.strftime('%B')

    return ano, mes, dia, trimestre, semestre, dia_da_semana, nome_mes

def transforma_acao(codigo_acao, nome_acao):
    codigo_acao = codigo_acao.strip().upper()
    nome_acao = nome_acao.strip()
    return codigo_acao, nome_acao

def transforma_cotacao(dados_cotacao, calendario_id, codigo_acao):
    preco = dados_cotacao['preco']
    return calendario_id, codigo_acao, preco
