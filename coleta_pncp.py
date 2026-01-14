import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys
import time

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Formatação de data para a URL do Governo (AAAAMMDD)
if env_inicio and env_fim:
    d_ini = env_inicio
    d_fim = env_fim
else:
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    d_ini, d_fim = ontem, ontem

print(f"--- BUSCA DE RESULTADOS PNCP: {d_ini} a {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# URL DE RESULTADOS DE ITENS (Esta é a URL que não dá 404)
URL_API = "https://pncp.gov.br/api/consulta/v1/itens/resultado"

params = {
    "dataResultadoInicial": d_ini,
    "dataResultadoFinal": d_fim,
    "codigoModalidadeContratacao": "6", # Pregão
    "pagina": 1,
    "tamanhoPagina": 100
}

try:
    # Chamada para a API de resultados
    resp = requests.get(URL_API, params=params, headers=HEADERS, timeout=60)
    
    if resp.status_code == 200:
        # A API retorna os dados dentro da chave 'data'
        dados = resp.json().get('data', [])
        print(f"✅ Sucesso! {len(dados)} itens homologados encontrados.")

        for item in dados:
            # Pegamos apenas itens que têm um fornecedor vencedor
            fornecedor = item.get('nomeRazaoSocialFornecedor')
            valor = item.get('valorTotalHomologado', 0)
            
            if fornecedor and valor > 0:
                uasg = str(item.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                seq = str(item.get('sequencialCompra', '00000')).zfill(5)
                ano = item.get('anoCompra')
                
                todos_itens.append({
                    "Data": d_ini,
                    "UASG": uasg,
                    "Orgao": item.get('orgaoEntidade', {}).get('razaoSocial', 'Órgão não identificado'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": fornecedor,
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(valor),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro na API: {resp.status_code}")
        print(f"Mensagem: {resp.text[:100]}")

except Exception as e:
    print(f"❌ Falha de conexão: {e}")

# --- PROCESSAMENTO E SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum item encontrado. Dica: Tente dias úteis recentes (ex: ontem).")
    sys.exit(0)

df = pd.DataFrame(todos_itens)
# Agrupar itens da mesma licitação para o mesmo fornecedor
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({
    'Itens': 'sum', 
    'Total': 'sum'
