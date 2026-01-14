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
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
    print(f"--- MODO MANUAL: Buscando HOMOLOGAÇÕES de {env_inicio} até {env_fim} ---")
else:
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n>>> PESQUISANDO RESULTADOS HOMOLOGADOS EM: {DATA_STR} <<<")
    
    # MUDANÇA CHAVE: Agora buscamos direto no endpoint de resultados por data
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/resultados"
    
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
            resultados = resp.json().get('data', [])
            print(f"  ✅ Encontrados {len(resultados)} itens homologados neste dia.")

            for item in resultados:
                # Captura os dados diretamente do resultado
                fornecedor = item.get('nomeRazaoSocialFornecedor')
                valor = item.get('valorTotalHomologado', 0)
                
                if fornecedor and valor > 0:
                    # Informações do Órgão e Licitação
                    uasg = str(item.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                    ano = item.get('anoCompra')
                    seq = item.get('sequencialCompra')
                    
                    todos_itens.append({
                        "Data": DATA_STR,
                        "UASG": uasg,
                        "Orgao": item.get('orgaoEntidade', {}).get('razaoSocial', 'Órgão não identificado'),
                        "Licitacao": f"{uasg}{str(seq).zfill(5)}{ano}",
                        "Fornecedor": fornecedor,
                        "CNPJ": item.get('niFornecedor', ''),
                        "Total": float(valor),
                        "Itens": 1 # Neste endpoint, cada linha é um item
                    })
        else:
            print(f"  ❌ Erro API: {resp.status_code}")
    except Exception as e:
        print(f"  ❌ Falha: {e}")

    data_atual += timedelta(days=1)
    time.sleep(0.5) # Evitar bloqueio

if not todos_itens:
    print("\n⚠️ Nenhum dado encontrado. Tente uma data mais recente (ex: ontem).")
    sys.exit(0)

# Agrupar para somar valores caso o mesmo fornecedor tenha ganho vários itens na mesma licitação
df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({
    'Itens': 'sum',
    'Total': 'sum'
}).reset_index()

novos_dados = agrupado.to_dict(orient='records')

# Salvar (Mantendo o histórico)
if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: historico = []
else: historico = []

historico.extend(novos_dados)
# Remover duplicatas baseadas em todos os campos
final_list = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final_list, f, indent=4, ensure_ascii=False)

print(f"💾 Sucesso! {len(final_list)} registros no total.")
