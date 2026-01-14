import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import sys

# --- CONFIGURAÇÃO DA NOVA API (DADOS ABERTOS COMPRAS.GOV) ---
BASE_URL = "https://dadosabertos.compras.gov.br/modulo-contratacoes"
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    # Formato esperado por esta API: AAAA-MM-DD
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
    print(f"--- BUSCA DADOS ABERTOS: {d_ini} a {d_fim} ---")
else:
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    d_ini = ontem
    d_fim = ontem

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# 1. Consultar Resultados de Itens (Endpoint 3)
url_resultados = f"{BASE_URL}/3_consultarResultadoItensContratacoes_PNCP_14133"

params = {
    "dataPublicacaoPncpInicial": d_ini,
    "dataPublicacaoPncpFinal": d_fim,
    "pagina": 1
}

try:
    print(f"Consultando resultados da API de Dados Abertos...")
    resp = requests.get(url_resultados, params=params, headers=HEADERS)
    
    if resp.status_code == 200:
        dados = resp.json().get('resultado', []) # Note que nesta API o campo é 'resultado'
        print(f"✅ Encontrados {len(dados)} itens com resultado.")

        for item in dados:
            # Filtramos apenas PREGÃO (A API de dados abertos costuma trazer a modalidade descrita)
            modalidade = str(item.get('modalidadeNome', '')).upper()
            if "PREGÃO" in modalidade or item.get('codigoModalidade') == 6:
                
                uasg = str(item.get('codigoUasg', '')).zfill(6)
                ano = item.get('anoCompra')
                seq = str(item.get('numeroCompra', '')).zfill(5)
                
                todos_itens.append({
                    "Data": item.get('dataPublicacaoPncp', '')[:10].replace('-', ''),
                    "UASG": uasg,
                    "Orgao": item.get('nomeOrgao', 'Não identificado'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": item.get('nomeRazaoSocialFornecedor', 'N/I'),
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(item.get('valorTotalHomologado', 0)),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro na API: {resp.status_code}")

except Exception as e:
    print(f"❌ Erro de conexão: {e}")

# --- SALVAMENTO (IGUAL ANTERIOR) ---
if not todos_itens:
    print("⚠️ Nenhum dado encontrado na nova API para este período.")
    sys.exit(0)

df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({'Itens': 'sum', 'Total': 'sum'}).reset_index()
novos_dados = agrupado.to_dict(orient='records')

if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: historico = []
else: historico = []

historico.extend(novos_dados)
final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final, f, indent=4, ensure_ascii=False)

print(f"💾 Sucesso! Banco de dados atualizado com a nova API.")
