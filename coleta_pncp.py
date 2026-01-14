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

if env_inicio and env_fim:
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
else:
    data_atual = datetime.now() - timedelta(days=1)
    data_limite = data_atual

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# --- FLUXO DE COLETA ---
while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    # O PNCP exige hifen na data para este endpoint: AAAA-MM-DD
    DATA_BUSCA = data_atual.strftime('%Y-%m-%d')
    
    print(f"\n>>> PESQUISANDO ITENS HOMOLOGADOS EM: {DATA_STR} <<<")
    
    # URL Estável de Consulta de Itens
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/itens"
    
    params = {
        "pagina": 1,
        "tamanhoPagina": 100,
        "dataAtualizacaoInicial": DATA_BUSCA,
        "dataAtualizacaoFinal": DATA_BUSCA,
        "codigoModalidadeContratacao": "6" # Pregão
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        
        if resp.status_code == 200:
            dados = resp.json().get('data', [])
            print(f"  ✅ Encontrados {len(dados)} registros atualizados.")

            for item in dados:
                # Verificamos se o item tem um vencedor (homologado)
                # No endpoint de itens, o campo geralmente é 'nomeRazaoSocialFornecedor'
                fornecedor = item.get('nomeRazaoSocialFornecedor')
                valor = item.get('valorTotalItem', 0)
                
                # Só salvamos se tiver fornecedor e o item estiver em situação de conclusão
                situacao = item.get('situacaoCompraItemResultadoNome', '')
                
                if fornecedor and valor > 0:
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
                        "Itens": 1
                    })
        else:
            print(f"  ❌ Erro API: {resp.status_code} - Verifique se a URL mudou.")
            
    except Exception as e:
        print(f"  ❌ Falha de conexão: {e}")

    data_atual += timedelta(days=1)
    time.sleep(1)

# --- PROCESSAMENTO FINAL ---
if not todos_itens:
    print("\n⚠️ A API não retornou itens homologados para este período.")
    sys.exit(0)

# Agrupamento
df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({
    'Itens': 'sum',
    'Total': 'sum'
}).reset_index()

novos_dados = agrupado.to_dict(orient='records')

# Salvar e manter histórico
if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: historico = []
else: historico = []

historico.extend(novos_dados)
# Remove duplicatas
final_list = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final_list, f, indent=4, ensure_ascii=False)

print(f"💾 Sucesso! {len(final_list)} registros totais no sistema.")
