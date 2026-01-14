import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURAÇÃO ---
# URL REAL DE PRODUÇÃO (Baseada no Gateway do Governo)
URL_API = "https://dadosabertos.compras.gov.br/modulo-contratacoes/v1/consultarResultadoItensContratacoes_PNCP_14133"

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Formato AAAAMMDD para a API de Dados Abertos
if env_inicio and env_fim:
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
else:
    d_ini = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    d_fim = datetime.now().strftime('%Y-%m-%d')

print(f"--- ACESSANDO DADOS ABERTOS GOV: {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# Parâmetros exatos conforme o Swagger
params = {
    "dataPublicacaoPncpInicial": d_ini,
    "dataPublicacaoPncpFinal": d_fim,
    "pagina": 1
}

try:
    # Testamos a URL com /v1/ que é o padrão de Gateway
    resp = requests.get(URL_API, params=params, headers=HEADERS, timeout=30)
    
    if resp.status_code == 200:
        # Se entrar aqui, o 404 foi resolvido!
        resultado = resp.json().get('resultado', [])
        print(f"✅ Sucesso! {len(resultado)} itens encontrados.")
        
        for item in resultado:
            if item.get('codigoModalidade') == 6: # Pregão
                uasg = str(item.get('codigoUasg', '')).zfill(6)
                seq = str(item.get('numeroCompra', '')).zfill(5)
                ano = item.get('anoCompra')
                
                todos_itens.append({
                    "Data": item.get('dataPublicacaoPncp', '')[:10].replace('-', ''),
                    "UASG": uasg,
                    "Orgao": item.get('nomeOrgao', 'Órgão Federal'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": item.get('nomeRazaoSocialFornecedor', 'N/I'),
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(item.get('valorTotalHomologado', 0)),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro {resp.status_code}. A API respondeu: {resp.text[:100]}")

except Exception as e:
    print(f"❌ Falha de Conexão: {e}")

# --- SALVAMENTO ---
if not todos_itens:
    print("⚠️ Nenhum dado capturado.")
    sys.exit(0)

# Agrupar e salvar (mantendo seu padrão)
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
