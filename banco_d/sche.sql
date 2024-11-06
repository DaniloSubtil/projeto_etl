CREATE DATABASE IF NOT EXISTS b3_etl;

USE b3_etl;

-- Tabela de Dimensão de Ações
CREATE TABLE IF NOT EXISTS Dim_Acao (
    id_acao INT AUTO_INCREMENT PRIMARY KEY,
    codigo_acao VARCHAR(10) NOT NULL,
    nome_acao VARCHAR(100) NOT NULL
);

-- Tabela de Dimensão de Calendário
CREATE TABLE IF NOT EXISTS Dim_Calendario (
    cd_dt INT AUTO_INCREMENT PRIMARY KEY,
    ano INT,
    mes INT,
    trimestre INT,
    semestre INT,
    semana INT
);

-- Tabela de Dimensão de Empresas
CREATE TABLE IF NOT EXISTS Dim_Empresa (
    id_empresa INT AUTO_INCREMENT PRIMARY KEY,
    nome_empresa VARCHAR(100) NOT NULL
);

-- Tabela de Dimensão de Intermediárias
CREATE TABLE IF NOT EXISTS Dim_Intermediaria (
    id_intermediaria INT AUTO_INCREMENT PRIMARY KEY,
    nome_intermediaria VARCHAR(100) NOT NULL
);

-- Tabela de Fato (Cotações Históricas)
CREATE TABLE IF NOT EXISTS Fato_Cotacoes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_acao INT,
    cd_dt INT,
    id_empresa INT,
    id_intermediaria INT,
    preco_abertura DECIMAL(15, 2),
    preco_fechamento DECIMAL(15, 2),
    quantidade INT,
    FOREIGN KEY (id_acao) REFERENCES Dim_Acao(id_acao),
    FOREIGN KEY (cd_dt) REFERENCES Dim_Calendario(cd_dt),
    FOREIGN KEY (id_empresa) REFERENCES Dim_Empresa(id_empresa),
    FOREIGN KEY (id_intermediaria) REFERENCES Dim_Intermediaria(id_intermediaria)
);

-- 1. Média do preço de fechamento por ano
SELECT dc.ano, AVG(fc.preco_fechamento) AS media_preco_fechamento
FROM Fato_Cotacoes fc
JOIN Dim_Calendario dc ON fc.cd_dt = dc.cd_dt
GROUP BY dc.ano;

-- 2. Total de volume de ações negociadas por trimestre
SELECT dc.ano, dc.trimestre, SUM(fc.quantidade) AS total_volume
FROM Fato_Cotacoes fc
JOIN Dim_Calendario dc ON fc.cd_dt = dc.cd_dt
GROUP BY dc.ano, dc.trimestre;

-- 3. Preço máximo de abertura e fechamento por empresa
SELECT de.nome_empresa, MAX(fc.preco_abertura) AS max_preco_abertura, MAX(fc.preco_fechamento) AS max_preco_fechamento
FROM Fato_Cotacoes fc
JOIN Dim_Empresa de ON fc.id_empresa = de.id_empresa
GROUP BY de.nome_empresa;

-- 4. Volume total de ações negociadas por mês
SELECT dc.ano, dc.mes, SUM(fc.quantidade) AS volume_mensal
FROM Fato_Cotacoes fc
JOIN Dim_Calendario dc ON fc.cd_dt = dc.cd_dt
GROUP BY dc.ano, dc.mes;

-- 5. Preço médio de fechamento para cada ação específica no último ano
SELECT da.codigo_acao, AVG(fc.preco_fechamento) AS preco_medio
FROM Fato_Cotacoes fc
JOIN Dim_Acao da ON fc.id_acao = da.id_acao
JOIN Dim_Calendario dc ON fc.cd_dt = dc.cd_dt
WHERE dc.ano = 2024
GROUP BY da.codigo_acao;

-- Consulta 6: Exibir todas as cotações de uma empresa específica em um período
SELECT * FROM fato_cotacao
JOIN dimensao_empresa ON fato_cotacao.empresa_id = dimensao_empresa.id
JOIN dimensao_data ON fato_cotacao.data_id = dimensao_data.id
WHERE dimensao_empresa.codigo = 'ABEV3'
AND dimensao_data.data BETWEEN '2023-01-01' AND '2023-12-31';