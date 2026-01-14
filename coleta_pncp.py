import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURAÇÃO ---
# Esta URL inclui o prefixo /v1/ que é o padrão de produção do Compras.gov.br
URL_BASE = "https://dadosabertos.compras.gov.br/modulo-contratacoes/v1/consultarContratacoes_PNCP_14133"

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Formatação exigida por essa API: AAAA-MM-DD
if env_inicio and env_fim:
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
else:
    d_ini = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    d_fim = datetime.now().strftime('%Y-%m-%d')

print(f"--- CONSULTA DADOS ABERTOS: {d_ini} até {d_fim} ---")

params = {
    "dataPublicacaoPncpInicial": d_ini,
    "dataPublicacaoPncpFinal": d_fim,
    "codigoModalidade": 6, # Pregão
    "pagina": 1
}

try:
    resp = requests.get(URL_BASE, params=params, headers=HEADERS, timeout=30)
    
    if resp.status_code == 200:
        dados = resp.json().get('resultado', [])
        print(f"✅ Sucesso! Encontradas {len(dados)} contratações.")
        
        # Aqui, como estamos no endpoint 1, pegamos os dados básicos da licitação
        # Se precisar do vencedor, o robô teria que consultar o endpoint 3 em seguida
        for item in dados:
            uasg = str(item.get('codigoUasg', '000000')).zfill(6)
            seq = str(item.get('numeroCompra', '00000')).zfill(5)
            ano = item.get('anoCompra')
            
            # Nota: O endpoint 1 traz dados do edital. 
            # Para o vencedor (Fornecedor), o ideal é o endpoint 3.
            print(f"  Encontrada: UASG {uasg} - Pregão {seq}/{ano}")
            
    elif resp.status_code == 404:
        print("❌ Erro 404: O caminho /v1/ não foi encontrado. Tentando sem o prefixo...")
        # Tentativa sem /v1/ caso o servidor mude
        url_alt = URL_BASE.replace("/v1/", "/")
        resp_alt = requests.get(url_alt, params=params, headers=HEADERS)
        print(f"  Resultado alternativa: {resp_alt.status_code}")
    else:
        print(f"❌ Erro {resp.status_code}: {resp.text}")

except Exception as e:
    print(f"❌ Falha: {e}")
