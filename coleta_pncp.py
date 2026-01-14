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

# Configuração de datas 
if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    # Se não informar datas, pega Outubro/2025 para teste
    d_ini = "20251001"
    d_fim = "20251005"

print(f"--- ROBÔ TURBO (BUSCA TOTAL): {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# MUDANÇA 1: Aumentamos para 100 páginas (5.000 licitações/dia)
# Isso garante que ele leia TUDO, mas pare antes se acabar os dados.
MAX_PAGINAS = 100 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while pagina <= MAX_PAGINAS:
        # 1. Busca Licitações HOMOLOGADAS (Status 4)
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "situacaoCompraId": "4",            # Só homologadas
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            licitacoes = resp.json().get('data', [])
            
            # MUDANÇA 2: Se a lista vier vazia, para o loop deste dia imediatamente
            if not licitacoes: 
                break
            
            print(f"[P{pagina}]", end=" ", flush=True)

            for lic in licitacoes:
                cnpj_orgao = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"

                # 2. MODO TURBO: Busca TODOS os resultados da compra de uma vez só
                url_res_global = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/resultados"
                
                try:
                    r_glob = requests.get(url_res_global, headers=HEADERS, timeout=15)
                    if r_glob.status_code == 200:
                        resultados = r_glob.json()
                        if isinstance(resultados, dict): resultados = [resultados]

                        for res in resultados:
                            cnpj_forn = res.get('niFornecedor')
                            nome_forn = res.get('nomeRazaoSocialFornecedor')
                            
                            if not nome_forn and cnpj_forn:
                                nome_forn = f"CNPJ {cnpj_forn}"
                            
                            valor = res.get('valorTotalHomologado')
                            if valor is None: valor = 0
                            
                            if cnpj_forn:
                                todos_itens.append({
                                    "Data": DATA_STR,
                                    "UASG": uasg,
                                    "Orgao": nome_orgao,
                                    "Licitacao": id_licitacao,
                                    "Fornecedor": nome_forn,
                                    "CNPJ": cnpj_forn,
                                    "Total": float(valor),
                                    "Itens": 1
                                })
                except: pass
            
            pagina += 1

        except: break
    
    data_atual += timedelta(days=1)

# --- SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum dado encontrado.")
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

print(f"\n✅ SUCESSO! Banco de dados atualizado com {len(final)} registros.")
