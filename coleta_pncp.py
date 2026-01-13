import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os

# --- CONFIGURAÇÕES ---
# Pega dados de ONTEM (D-1)
DATA_BUSCA = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
ARQUIVO_SAIDA = 'dados.json'

print(f"--- INICIANDO ROBÔ TURBO: {DATA_BUSCA} ---")

todos_itens_ganhos = []

# ==============================================================================
# 1. BUSCAR TODAS AS CONTRATAÇÕES (PAGINAÇÃO AUTOMÁTICA)
# ==============================================================================
url_base_contratacoes = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
lista_contratacoes = []
pagina_atual = 1

while True:
    print(f"Baixando página {pagina_atual} de licitações...")
    params = {
        "dataInicial": DATA_BUSCA,
        "dataFinal": DATA_BUSCA,
        "pagina": pagina_atual,
        "tamanhoPagina": 50 # Pede de 50 em 50 para não travar
    }
    
    try:
        resp = requests.get(url_base_contratacoes, params=params)
        
        # Se der erro ou não tiver mais nada, para.
        if resp.status_code != 200:
            print(f"Fim das páginas ou erro: Status {resp.status_code}")
            break
            
        dados = resp.json().get('data', [])
        
        if not dados: # Lista vazia, acabou.
            print("Página vazia. Fim da coleta.")
            break
            
        lista_contratacoes.extend(dados)
        
        # Proteção de Loop Infinito (opcional: limitar a 20 páginas para teste)
        # if pagina_atual >= 20: break 
        
        pagina_atual += 1
        time.sleep(0.3) # Respira para não bloquear
        
    except Exception as e:
        print(f"Erro na página {pagina_atual}: {e}")
        break

print(f"> Total de licitações encontradas no dia: {len(lista_contratacoes)}")

# ==============================================================================
# 2. ENTRAR EM CADA LICITAÇÃO E PEGAR RESULTADOS
# ==============================================================================
# Limitador de segurança para o GitHub Actions gratuito (Processa max 300 para não estourar tempo)
# Se quiser processar tudo, remova o [:300]
lista_processamento = lista_contratacoes[:300] 

for i, compra in enumerate(lista_processamento):
    try:
        orgao_cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
        ano_compra = compra.get('anoCompra')
        seq_compra = compra.get('sequencialCompra')
        nome_orgao = compra.get('orgaoEntidade', {}).get('razaoSocial')
        
        if not (orgao_cnpj and ano_compra and seq_compra):
            continue

        num_licitacao = f"{seq_compra}/{ano_compra}"

        # Endpoint de Resultados
        url_resultados = f"https://pncp.gov.br/api/pncp/v1/orgaos/{orgao_cnpj}/compras/{ano_compra}/{seq_compra}/itens/resultados"
        
        resp_res = requests.get(url_resultados)
        
        if resp_res.status_code == 200:
            itens = resp_res.json()
            # Se vier só um dicionário (bug comum da API), transforma em lista
            if isinstance(itens, dict): itens = [itens]
            
            for item in itens:
                # Pega apenas HOMOLOGADOS (Vencedores reais)
                if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                    item['_num_licitacao'] = num_licitacao
                    item['_or
