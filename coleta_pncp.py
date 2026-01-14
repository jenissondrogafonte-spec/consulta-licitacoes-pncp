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
    # Formato exigido por este endpoint: AAAAMMDD
    DATA_STR = data_atual.strftime('%Y%m%d')
    
    print(f"\n>>> PESQUISANDO RESULTADOS EM: {DATA_STR} <<<")
    
    # URL DEFINITIVA: Consulta de itens por data de resultado
    # Esta é a URL que alimenta a pesquisa pública do PNCP
    url = "https://pncp.gov.br/api/consulta/v1/itens/resultado"
    
    params = {
        "pagina": 1,
        "tamanhoPagina": 100,
        "dataResultadoInicial": DATA_STR,
        "dataResultadoFinal": DATA_STR,
        "codigoModalidadeContratacao": "6" # Pregão
    }

    try:
        # Note que não usamos hífens na data aqui, pois esta API prefere o número puro
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        
        if resp.status_code == 200:
            dados = resp.json().get('data', [])
            print(f"  ✅ Sucesso! Encontrados {len(dados)} itens nesta data.")

            for item in dados:
                fornecedor = item.get('nomeRazaoSocialFornecedor')
                valor = item.get('valorTotalHomologado', 0)
                
                # Só pegamos se tiver valor e fornecedor
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
            print(f"  ❌ Erro API: {resp.status_code}. Tentando formato com hífens...")
            # Tentativa de correção automática para o formato AAAA-MM-DD
            params["dataResultadoInicial"] = data_atual.strftime('%Y-%m-%d')
            params["dataResultadoFinal"] = data_atual.strftime('%Y-%m-%d')
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            
            if resp.status_code == 200:
                print("  ✅ Formato com hífens funcionou!")
                # ... repete a lógica de processamento ...
                dados = resp.json().get('data', [])
                for item in dados:
                    fornecedor = item.get('nomeRazaoSocialFornecedor')
                    if fornecedor:
                        uasg = str(item.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                        todos_itens.append({
                            "Data": DATA_STR, "UASG": uasg, 
                            "Orgao": item.get('orgaoEntidade', {}).get('razaoSocial', ''),
                            "Licitacao": f"{uasg}{str(item.get('sequencialCompra')).zfill(5)}{item.get('anoCompra')}",
                            "Fornecedor": fornecedor, "CNPJ": item.get('niFornecedor', ''),
                            "Total": float(item.get('valorTotalHomologado', 0)), "Itens": 1
                        })
            else:
                print(f"  ❌ Falha total no dia {DATA_STR} (Status {resp.status_code})")

    except Exception as e:
        print(f"  ❌ Erro de conexão: {e}")

    data_atual += timedelta(days=1)
    time.sleep(0.5)

# --- PROCESSAMENTO FINAL ---
if not todos_itens:
    print("\n⚠️ Nenhuma informação capturada. Verifique se as datas são dias úteis.")
    sys.exit(0)

# Agrupamento (Somar valores da mesma licitação para o mesmo fornecedor)
df = pd.DataFrame(todos_itens)
agrupado = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({
    'Itens': 'sum',
    'Total': 'sum'
}).reset_index()

novos_dados = agrupado.to_dict(orient='records')

# Salvar Histórico
if os.path.exists(ARQUIVO_SAIDA):
    with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: historico = []
else: historico = []

historico.extend(novos_dados)
final_list = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final_list, f, indent=4, ensure_ascii=False)

print(f"💾 Concluído! Banco de dados agora tem {len(final_list)} registros.")
