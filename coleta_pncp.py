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

if env_inicio and env_fim:
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
else:
    data_atual = datetime.now() - timedelta(days=2)
    data_limite = data_atual

print(f"--- COLETA SEGURA: {data_atual.strftime('%d/%m/%Y')} a {data_limite.strftime('%d/%m/%Y')} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# Loop dia a dia
while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"Consultando dia {DATA_STR}...", end=" ", flush=True)
    
    # URL de Publicação (A mais estável do PNCP)
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    params = {
        "dataInicial": DATA_STR,
        "dataFinal": DATA_STR,
        "codigoModalidadeContratacao": "6", # Pregão
        "pagina": 1,
        "tamanhoPagina": 50
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        
        if resp.status_code == 200:
            licitacoes = resp.json().get('data', [])
            encontrados_dia = 0
            
            for lic in licitacoes:
                cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')

                # Busca os vencedores dentro desta licitação específica
                url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                
                try:
                    r_res = requests.get(url_res, headers=HEADERS, timeout=15)
                    if r_res.status_code == 200:
                        resultados = r_res.json()
                        if isinstance(resultados, dict): resultados = [resultados]
                        
                        for it in resultados:
                            if it.get('nomeRazaoSocialFornecedor'):
                                todos_itens.append({
                                    "Data": DATA_STR,
                                    "UASG": uasg,
                                    "Orgao": nome_orgao,
                                    "Licitacao": f"{uasg}{str(seq).zfill(5)}{ano}",
                                    "Fornecedor": it.get('nomeRazaoSocialFornecedor'),
                                    "CNPJ": it.get('niFornecedor', ''),
                                    "Total": float(it.get('valorTotalHomologado', 0)),
                                    "Itens": 1
                                })
                                encontrados_dia += 1
                except:
                    continue # Se uma licitação falhar, pula para a próxima
                
                time.sleep(0.1) # Evita sobrecarga
            
            print(f"OK ({encontrados_dia} homologações encontradas)")
        else:
            print(f"Erro {resp.status_code}")

    except Exception as e:
        print(f"Falha de conexão")

    data_atual += timedelta(days=1)
    time.sleep(0.5)

# --- SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhuma homologação encontrada. Lembre-se: licitações muito recentes podem ainda não ter resultado.")
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

print(f"\n💾 Sucesso! Banco de dados atualizado.")
