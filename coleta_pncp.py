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

# Se não houver data, busca os últimos 2 dias
if env_inicio and env_fim:
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
else:
    data_atual = datetime.now() - timedelta(days=2)
    data_limite = datetime.now()

print(f"--- COLETA OTIMIZADA: {data_atual.strftime('%d/%m/%Y')} a {data_limite.strftime('%d/%m/%Y')} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# Loop dia a dia para evitar sobrecarga e Timeout
while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    # Para o endpoint de itens, o formato é AAAA-MM-DD
    DATA_BUSCA = data_atual.strftime('%Y-%m-%d')
    
    print(f"Busca: {DATA_STR}...", end=" ", flush=True)
    
    # Endpoint de Itens (Mais leve e direto ao ponto do resultado)
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/itens"
    
    params = {
        "pagina": 1,
        "tamanhoPagina": 100,
        "dataAtualizacaoInicial": DATA_BUSCA,
        "dataAtualizacaoFinal": DATA_BUSCA,
        "codigoModalidadeContratacao": "6" # Pregão
    }

    try:
        # Aumentamos o timeout para 60 segundos
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        
        if resp.status_code == 200:
            itens = resp.json().get('data', [])
            encontrados = 0
            for it in itens:
                fornecedor = it.get('nomeRazaoSocialFornecedor')
                if fornecedor:
                    uasg = str(it.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                    seq = str(it.get('sequencialCompra', '00000')).zfill(5)
                    ano = it.get('anoCompra')
                    
                    todos_itens.append({
                        "Data": DATA_STR,
                        "UASG": uasg,
                        "Orgao": it.get('orgaoEntidade', {}).get('razaoSocial', ''),
                        "Licitacao": f"{uasg}{seq}{ano}",
                        "Fornecedor": fornecedor,
                        "CNPJ": it.get('niFornecedor', ''),
                        "Total": float(it.get('valorTotalItem', 0)),
                        "Itens": 1
                    })
                    encontrados += 1
            print(f"OK ({encontrados} itens)")
        else:
            print(f"Erro {resp.status_code}")
            
    except Exception as e:
        print(f"Falha (Timeout ou Conexão)")

    data_atual += timedelta(days=1)
    time.sleep(1) # Pausa de 1 segundo entre dias para não ser bloqueado

# --- SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum dado capturado. Tente um intervalo menor (máx 7 dias).")
    sys.exit(0)

# Agrupamento para somar itens do mesmo fornecedor na mesma licitação
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
# Remove duplicados
final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final, f, indent=4, ensure_ascii=False)

print(f"\n💾 Sucesso! Banco de dados atualizado. Total de registros: {len(final)}")
