import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import sys

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

# Parâmetros de Data
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    env_inicio = env_inicio.replace('/', '').replace('-', '').replace(' ', '')
    env_fim = env_fim.replace('/', '').replace('-', '').replace(' ', '')
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
else:
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n>>> COLETANDO PREGÕES EM: {DATA_STR} <<<")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    params = {
        "dataInicial": DATA_STR,
        "dataFinal": DATA_STR,
        "codigoModalidadeContratacao": "6", # PREGÃO
        "pagina": 1,
        "tamanhoPagina": 50
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS)
        if resp.status_code == 200:
            licitacoes = resp.json().get('data', [])
            for compra in licitacoes:
                cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
                ano = compra.get('anoCompra')
                seq = compra.get('sequencialCompra')
                # UASG vem como 'codigoUnidade'
                uasg_limpa = str(compra.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                nome_org = compra.get('orgaoEntidade', {}).get('razaoSocial', '')

                if cnpj and ano and seq:
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                    r_res = requests.get(url_res, headers=HEADERS)
                    if r_res.status_code == 200:
                        itens = r_res.json()
                        if isinstance(itens, dict): itens = [itens]
                        for item in itens:
                            if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                                item['_data'] = DATA_STR
                                item['_uasg'] = uasg_limpa
                                item['_orgao_nome'] = nome_org
                                # Lógica ComprasNet: UASG + Sequencial + Ano (sem barras)
                                item['_lic_comprasnet'] = f"{uasg_limpa}{seq}{ano}"
                                todos_itens.append(item)
                    time.sleep(0.1)
    except Exception as e:
        print(f"Erro: {e}")
    data_atual += timedelta(days=1)

if not todos_itens:
    print("Nenhum dado novo encontrado.")
    sys.exit(0)

df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_lic_comprasnet', '_orgao_nome', '_uasg', '_data']).agg({
    'numeroItem': 'count', 'valorTotalHomologado': 'sum'
}).reset_index()

novos_dados = []
for _, row in agrupado.iterrows():
    novos_dados.append({
        "Data": row['_data'],
        "UASG": row['_uasg'],
        "Orgao": row['_orgao_nome'],
        "Licitacao": row['_lic_comprasnet'],
        "Fornecedor": row['nomeRazaoSocialFornecedor'],
        "CNPJ": row['niFornecedor'],
        "Total": float(row['valorTotalHomologado']),
        "Itens": int(row['numeroItem'])
    })

if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f: historico = json.load(f)
else: historico = []

historico.extend(novos_dados)
historico_final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(historico_final, f, indent=4, ensure_ascii=False)

print(f"💾 Sucesso! Banco de dados atualizado.")
