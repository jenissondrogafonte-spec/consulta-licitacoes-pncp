import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import sys

# --- CONFIGURAÇÃO ---
# URL completa extraída do Swagger (Módulo 07 - Endpoint 3)
URL_API = "https://dadosabertos.compras.gov.br/modulo-contratacoes/v1/consultarResultadoItensContratacoes_PNCP_14133"
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
else:
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    d_ini, d_fim = ontem, ontem

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# Parâmetros conforme documentação Swagger
params = {
    "dataPublicacaoPncpInicial": d_ini,
    "dataPublicacaoPncpFinal": d_fim,
    "pagina": 1
}

print(f"--- CONSULTANDO DADOS ABERTOS (V1) ---")
print(f"Período: {d_ini} até {d_fim}")

try:
    # A API de Dados Abertos é robusta, mas exige precisão nos parâmetros
    resp = requests.get(URL_API, params=params, headers=HEADERS, timeout=60)
    
    if resp.status_code == 200:
        # No Swagger, o retorno costuma vir em 'resultado' ou direto na lista
        conteudo = resp.json()
        itens = conteudo.get('resultado', []) if isinstance(conteudo, dict) else []
        
        print(f"✅ Sucesso! {len(itens)} itens processados pela API.")

        for item in itens:
            # Filtro para Pregão (Modalidade 6)
            if item.get('codigoModalidade') == 6 or "PREGÃO" in str(item.get('modalidadeNome', '')).upper():
                uasg = str(item.get('codigoUasg', '000000')).zfill(6)
                ano = item.get('anoCompra')
                seq = str(item.get('numeroCompra', '00000')).zfill(5)
                
                todos_itens.append({
                    "Data": str(item.get('dataPublicacaoPncp', d_ini))[:10].replace('-', ''),
                    "UASG": uasg,
                    "Orgao": item.get('nomeOrgao', 'Órgão Federal'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": item.get('nomeRazaoSocialFornecedor', 'N/I'),
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(item.get('valorTotalHomologado', 0)),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro {resp.status_code}: Verifique se o serviço está online.")
        print(f"Resposta: {resp.text[:200]}")

except Exception as e:
    print(f"❌ Falha de Conexão: {e}")

# --- SALVAMENTO ---
if not todos_itens:
    print("⚠️ A busca não retornou pregões homologados para este período.")
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

print(f"💾 Banco de dados atualizado! Total: {len(final)} registros.")
