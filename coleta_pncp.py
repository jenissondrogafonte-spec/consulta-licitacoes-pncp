import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os
import sys

# --- CONFIGURAÇÃO BLINDADA ---
# Cabeçalhos para fingir ser um navegador (Evita bloqueio 400/403)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

# Leitura e Limpeza dos Inputs
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Remove barras, traços e espaços extras
if env_inicio and env_fim:
    env_inicio = env_inicio.replace('/', '').replace('-', '').replace(' ', '')
    env_fim = env_fim.replace('/', '').replace('-', '').replace(' ', '')
    
    try:
        data_atual = datetime.strptime(env_inicio, '%Y%m%d')
        data_limite = datetime.strptime(env_fim, '%Y%m%d')
        print(f"--- MODO MANUAL ATIVADO: {env_inicio} até {env_fim} ---")
    except ValueError:
        print("❌ ERRO CRÍTICO: Formato de data inválido! Use AAAAMMDD (Ex: 20250204)")
        sys.exit(1)
else:
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem
    print(f"--- MODO AUTOMÁTICO (ONTEM): {data_ontem.strftime('%Y%m%d')} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# --- LOOP DE COLETA ---
while data_atual <= data_limite:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n>>> PROCESSANDO DIA: {DATA_STR} <<<")
    
    url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    # Tenta pegar apenas 10 por vez para não estressar a API
    params = {
        "dataInicial": DATA_STR,
        "dataFinal": DATA_STR,
        "pagina": 1,
        "tamanhoPagina": 10
    }

    try:
        print(f"  [1] Consultando API...")
        resp = requests.get(url, params=params, headers=HEADERS) # Adicionado Headers
        
        if resp.status_code != 200:
            print(f"  ❌ Erro na API: {resp.status_code} - {resp.text}")
            # Se der erro 400, tenta formato com traços (Plano B)
            if resp.status_code == 400:
                print("  ⚠️ Tentando formato alternativo (AAAA-MM-DD)...")
                DATA_STR_ALT = data_atual.strftime('%Y-%m-%d')
                params['dataInicial'] = DATA_STR_ALT
                params['dataFinal'] = DATA_STR_ALT
                resp = requests.get(url, params=params, headers=HEADERS)
                if resp.status_code == 200:
                    print("  ✅ Formato alternativo funcionou!")
                else:
                    print("  ❌ Falha também no formato alternativo.")
                    break
            else:
                break
        
        licitacoes = resp.json().get('data', [])
        print(f"  ✅ Encontradas {len(licitacoes)} licitações. Baixando resultados...")

        count_processados = 0
        for compra in licitacoes:
            # Pega IDs
            cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
            ano = compra.get('anoCompra')
            seq = compra.get('sequencialCompra')
            
            if cnpj and ano and seq:
                url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                try:
                    r_res = requests.get(url_res, headers=HEADERS)
                    if r_res.status_code == 200:
                        itens = r_res.json()
                        if isinstance(itens, dict): itens = [itens]
                        
                        for item in itens:
                            # Tira filtros para garantir que venha ALGO
                            item['_data'] = DATA_STR
                            item['_orgao'] = f"{cnpj} - {compra.get('orgaoEntidade', {}).get('razaoSocial')[:30]}..."
                            item['_licitacao'] = f"{seq}/{ano}"
                            item['_processo'] = compra.get('processo', '-')
                            todos_itens.append(item)
                            count_processados += 1
                except:
                    pass
            
            # Limite de segurança para teste (pega só os primeiros 50 do dia)
            if count_processados > 50: 
                break
            time.sleep(0.1)

    except Exception as e:
        print(f"  ❌ Erro de Conexão: {e}")

    data_atual += timedelta(days=1)

# --- SALVAR E FINALIZAR ---
print(f"\n📊 RESUMO: {len(todos_itens)} itens coletados.")

if not todos_itens:
    print("⚠️ A lista está vazia. O arquivo NÃO será alterado para evitar apagar dados antigos.")
    sys.exit(0) # Sai sem erro, mas não salva

# Processa e Salva
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

# Carga Incremental
if os.path.exists(ARQUIVO_SAIDA):
    try:
        with open(ARQUIVO_SAIDA, 'r') as f: historico = json.load(f)
    except: historico = []
else: historico = []

historico.extend(novos_dados)
# Remove duplicatas
historico_unico = list({json.dumps(i, sort_keys=True) for i in historico})
final = [json.loads(i) for i in historico_unico]

with open(ARQUIVO_SAIDA, 'w') as f:
    json.dump(final, f, indent=4)

print(f"💾 SUCESSO! {len(final)} registros no total.")
