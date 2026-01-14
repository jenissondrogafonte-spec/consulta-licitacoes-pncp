import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    d_ini = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
    d_fim = datetime.now().strftime('%Y%m%d')

print(f"--- BUSCA INTEGRADA PNCP + COMPRAS.GOV: {d_ini} a {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# Passo 1: Pegar a lista de licitações no PNCP (Endpoint estável)
URL_LISTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
params = {
    "dataInicial": d_ini,
    "dataFinal": d_fim,
    "codigoModalidadeContratacao": "6",
    "pagina": 1,
    "tamanhoPagina": 50
}

try:
    resp = requests.get(URL_LISTA, params=params, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        licitacoes = resp.json().get('data', [])
        print(f"✅ {len(licitacoes)} licitações encontradas no PNCP.")

        for lic in licitacoes:
            cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
            ano = lic.get('anoCompra')
            seq = lic.get('sequencialCompra')
            uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()

            # Passo 2: Buscar o resultado no endpoint de ITENS do PNCP (Mais estável que o de resultados)
            # URL: /pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens
            url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
            
            try:
                r_itens = requests.get(url_itens, headers=HEADERS, timeout=15)
                if r_itens.status_code == 200:
                    itens = r_itens.json()
                    for it in itens:
                        # Verificamos se o item tem um fornecedor vencedor informado
                        # No Compras.gov/PNCP, o resultado aparece aqui:
                        fornecedor = it.get('nomeRazaoSocialFornecedor')
                        if fornecedor:
                            todos_itens.append({
                                "Data": d_ini,
                                "UASG": uasg,
                                "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial', ''),
                                "Licitacao": f"{uasg}{str(seq).zfill(5)}{ano}",
                                "Fornecedor": fornecedor,
                                "CNPJ": it.get('niFornecedor', ''),
                                "Total": float(it.get('valorTotalItem', 0)),
                                "Itens": 1
                            })
            except: continue
    else:
        print(f"❌ Erro ao listar licitações: {resp.status_code}")

except Exception as e:
    print(f"❌ Erro: {e}")

# --- SALVAMENTO ---
if not todos_itens:
    print("⚠️ Nenhuma homologação encontrada (Normal para datas muito recentes).")
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

print(f"💾 Banco de dados atualizado com {len(final)} registros.")
