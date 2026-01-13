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

# Parâmetros de Data
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    env_inicio = env_inicio.replace('/', '').replace('-', '')
    env_fim = env_fim.replace('/', '').replace('-', '')
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
else:
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# --- LOOP DE COLETA ---
while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n>>> PESQUISANDO PREGÕES EM: {DATA_STR} <<<")
    
    # URL de Publicação (Voltou a funcionar com a modalidade correta)
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    params = {
        "dataInicial": DATA_STR,
        "dataFinal": DATA_STR,
        "codigoModalidadeContratacao": "6", # 6 é o código para PREGÃO
        "pagina": 1,
        "tamanhoPagina": 50
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS)
        
        if resp.status_code == 200:
            licitacoes = resp.json().get('data', [])
            print(f"  ✅ Encontrados {len(licitacoes)} Pregões publicados.")

            for compra in licitacoes:
                cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
                ano = compra.get('anoCompra')
                seq = compra.get('sequencialCompra')
                
                if cnpj and ano and seq:
                    # Busca os vencedores (Resultados)
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                    r_res = requests.get(url_res, headers=HEADERS)
                    
                    if r_res.status_code == 200:
                        itens = r_res.json()
                        if isinstance(itens, dict): itens = [itens]
                        
                        for item in itens:
                            if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                                item['_data'] = DATA_STR
                                item['_orgao'] = f"{cnpj} - {compra.get('orgaoEntidade', {}).get('razaoSocial')[:40]}"
                                item['_licitacao'] = f"{seq}/{ano}"
                                item['_processo'] = compra.get('processo', '-')
                                todos_itens.append(item)
                    time.sleep(0.1)
        else:
            print(f"  ❌ Erro {resp.status_code}: {resp.text}")

    except Exception as e:
        print(f"  ❌ Falha: {e}")

    data_atual += timedelta(days=1)

# --- SALVAR DADOS ---
if not todos_itens:
    print("\n⚠️ Nenhum resultado homologado encontrado nesta data.")
    sys.exit(0)

print(f"\n📊 Sucesso: {len(todos_itens)} itens de Pregão coletados.")

# Processamento e agrupamento
df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_licitacao', '_processo', '_orgao', '_data']).agg({
    'numeroItem': 'count', 'valorTotalHomologado': 'sum'
}).reset_index()

novos_dados = []
for _, row in agrupado.iterrows():
    novos_dados.append({
        "Data_Homologacao": row['_data'],
        "Orgao_Codigo": row['_orgao'],
        "Num_Licitacao": row['_licitacao'],
        "Num_Processo": row['_processo'],
        "Fornecedor": row['nomeRazaoSocialFornecedor'],
        "CNPJ_Fornecedor": row['niFornecedor'],
        "Total_Ganho_R$": float(row['valorTotalHomologado']),
        "Itens_Ganhos": int(row['numeroItem'])
    })

# Atualização do arquivo JSON
if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r') as f: historico = json.load(f)
else: historico = []

historico.extend(novos_dados)
# Limpeza de duplicados
historico_final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w') as f:
    json.dump(historico_final, f, indent=4)

print("💾 Banco de dados atualizado!")
