import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import sys

# --- CONFIGURAÇÃO ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Formatação de datas para a API do PNCP (AAAA-MM-DD)
if env_inicio and env_fim:
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
else:
    d_ini = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    d_fim = datetime.now().strftime('%Y-%m-%d')

print(f"--- CONSULTA CONSOLIDADA PNCP: {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# URL Estável de Consulta do Governo
URL_BASE = "https://pncp.gov.br/api/consulta/v1/contratacoes"

params = {
    "dataAtualizacaoInicial": d_ini,
    "dataAtualizacaoFinal": d_fim,
    "codigoModalidadeContratacao": "6", # Pregão
    "pagina": 1,
    "tamanhoPagina": 50
}

try:
    # 1. Busca as contratações atualizadas no período
    resp = requests.get(URL_BASE, params=params, headers=HEADERS, timeout=30)
    
    if resp.status_code == 200:
        contratacoes = resp.json().get('data', [])
        print(f"✅ Encontradas {len(contratacoes)} contratações atualizadas.")

        for c in contratacoes:
            cnpj = c.get('orgaoEntidade', {}).get('cnpj')
            ano = c.get('anoCompra')
            seq = c.get('sequencialCompra')
            uasg = str(c.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
            nome_orgao = c.get('orgaoEntidade', {}).get('razaoSocial', '')

            # 2. Busca os resultados de cada uma
            url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
            
            try:
                r_res = requests.get(url_res, headers=HEADERS, timeout=15)
                if r_res.status_code == 200:
                    itens = r_res.json()
                    if isinstance(itens, dict): itens = [itens]
                    
                    for it in itens:
                        # Pegamos apenas quem tem fornecedor e valor (Homologado)
                        fornecedor = it.get('nomeRazaoSocialFornecedor')
                        valor = it.get('valorTotalHomologado', 0)
                        
                        if fornecedor and valor > 0:
                            todos_itens.append({
                                "Data": d_ini.replace('-', ''),
                                "UASG": uasg,
                                "Orgao": nome_orgao,
                                "Licitacao": f"{uasg}{str(seq).zfill(5)}{ano}",
                                "Fornecedor": fornecedor,
                                "CNPJ": it.get('niFornecedor', ''),
                                "Total": float(valor),
                                "Itens": 1
                            })
                time.sleep(0.1) # Evita bloqueio
            except:
                continue
    else:
        print(f"❌ Erro {resp.status_code} na URL Base.")

except Exception as e:
    print(f"❌ Erro de Conexão: {e}")

# --- SALVAMENTO ---
if not todos_itens:
    print("⚠️ Nenhum item homologado encontrado para as contratações deste período.")
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

print(f"💾 Sucesso! {len(final)} registros salvos.")
