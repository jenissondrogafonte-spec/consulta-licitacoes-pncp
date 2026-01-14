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
    # A API de Itens exige o formato AAAA-MM-DD
    d_ini = f"{env_inicio[:4]}-{env_inicio[4:6]}-{env_inicio[6:8]}"
    d_fim = f"{env_fim[:4]}-{env_fim[4:6]}-{env_fim[6:8]}"
    data_atual_dt = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite_dt = datetime.strptime(env_fim, '%Y%m%d')
else:
    # Padrão: Últimos 2 dias
    data_atual_dt = datetime.now() - timedelta(days=2)
    data_limite_dt = data_atual_dt
    d_ini = data_atual_dt.strftime('%Y-%m-%d')
    d_fim = d_ini

print(f"--- BUSCA POR RESULTADOS (HOMOLOGADOS): {d_ini} a {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# URL DE ITENS: É a mais completa e traz o vencedor diretamente
# Usamos dataAtualizacao para pegar tudo que foi "batido o martelo" no período
url = "https://pncp.gov.br/api/consulta/v1/contratacoes/itens"

params = {
    "pagina": 1,
    "tamanhoPagina": 100,
    "dataAtualizacaoInicial": d_ini,
    "dataAtualizacaoFinal": d_fim,
    "codigoModalidadeContratacao": "6" # Pregão
}

try:
    # 1. Faz a chamada principal para pegar todos os itens ganhos no período
    resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
    
    if resp.status_code == 200:
        dados = resp.json().get('data', [])
        print(f"✅ Sucesso! {len(dados)} itens atualizados encontrados.")

        for item in dados:
            # SÓ PROCESSA SE TIVER FORNECEDOR (Ou seja, se já houve homologação)
            fornecedor = item.get('nomeRazaoSocialFornecedor')
            valor = item.get('valorTotalItem', 0)
            
            if fornecedor and valor > 0:
                # Dados da Licitação para montar a Identificação (UASG+SEQ+ANO)
                uasg = str(item.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                seq = str(item.get('sequencialCompra', '00000')).zfill(5)
                ano = item.get('anoCompra')
                
                # Data da atualização (vira a data de registro no nosso banco)
                data_reg = item.get('dataAtualizacao', d_ini)[:10].replace('-', '')

                todos_itens.append({
                    "Data": data_reg,
                    "UASG": uasg,
                    "Orgao": item.get('orgaoEntidade', {}).get('razaoSocial', 'Órgão não identificado'),
                    "Licitacao": f"{uasg}{seq}{ano}",
                    "Fornecedor": fornecedor,
                    "CNPJ": item.get('niFornecedor', ''),
                    "Total": float(valor),
                    "Itens": 1
                })
    else:
        print(f"❌ Erro na API: {resp.status_code}")

except Exception as e:
    print(f"❌ Falha de conexão: {e}")

# --- SALVAMENTO E AGRUPAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum item homologado encontrado com esses critérios.")
    sys.exit(0)

# Agrupa para somar itens se o mesmo fornecedor ganhou vários na mesma licitação
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
# Remove duplicatas exatas
final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(final, f, indent=4, ensure_ascii=False)

print(f"💾 Banco de dados atualizado! Total de {len(final)} registros históricos.")
