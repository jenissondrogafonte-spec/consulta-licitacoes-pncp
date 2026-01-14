import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Formatação: Este endpoint exige AAAA-MM-DD
if env_inicio and env_fim:
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
else:
    # Padrão: Ontem
    data_ontem = datetime.now() - timedelta(days=1)
    d_ini = data_ontem.strftime('%Y-%m-%d')
    d_fim = d_ini

print(f"--- CONSULTA CONSOLIDADA PNCP: {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# URL ESTÁVEL: Consulta de itens de contratações
URL_API = "https://pncp.gov.br/api/consulta/v1/contratacoes/itens"

params = {
    "pagina": 1,
    "tamanhoPagina": 100,
    "dataAtualizacaoInicial": d_ini,
    "dataAtualizacaoFinal": d_fim,
    "codigoModalidadeContratacao": "6" # Pregão
}

try:
    resp = requests.get(URL_API, params=params, headers=HEADERS, timeout=60)
    
    if resp.status_code == 200:
        dados = resp.json().get('data', [])
        print(f"✅ Sucesso! {len(dados)} registros encontrados.")

        for item in dados:
            # Capturamos apenas se já houver um vencedor (Fornecedor)
            fornecedor = item.get('nomeRazaoSocialFornecedor')
            valor = item.get('valorTotalItem', 0)
            
            if fornecedor and valor > 0:
                uasg = str(item.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                seq = str(item.get('sequencialCompra', '00000')).zfill(5)
                ano = item.get('anoCompra')
                
                todos_itens.append({
                    "Data": d_ini.replace('-', ''),
                    "UASG": uasg,
                    "Orgao": item.get('orgaoEntidade', {}).get('razaoSocial', 'Órgão não identificado'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": fornecedor,
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(valor),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro na API: {resp.status_code} - URL pode ter mudado.")

except Exception as e:
    print(f"❌ Falha de conexão: {e}")

# --- SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum item homologado encontrado com esses critérios.")
    sys.exit(0)

df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({
    'Itens': 'sum', 
    'Total': 'sum'
}).reset_index()

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

print(f"💾 Banco de dados atualizado! Total: {len(final)} registros.")
