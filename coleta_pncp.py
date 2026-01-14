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

# Usa Outubro/2025 para garantir (período com muitas homologações)
if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    d_ini = "20251001"
    d_fim = "20251005"

print(f"--- ROBÔ FILTRADO (SÓ HOMOLOGADAS): {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []
MAX_PAGINAS = 5 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while pagina <= MAX_PAGINAS:
        # 1. Busca APENAS Licitações HOMOLOGADAS (Id 4)
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "situacaoCompraId": "4",            # <--- O PULO DO GATO: Filtra só o que já acabou
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            licitacoes = resp.json().get('data', [])
            if not licitacoes: break # Se não tem homologadas nessa página, acabou
            
            print(f"[P{pagina} - {len(licitacoes)} itens]", end=" ", flush=True)

            for lic in licitacoes:
                cnpj_orgao = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"

                # 2. Busca Lista de Itens
                url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens"
                
                try:
                    r_it = requests.get(url_itens, headers=HEADERS, timeout=15)
                    if r_it.status_code == 200:
                        lista_itens = r_it.json()
                        
                        for it in lista_itens:
                            # Se a licitação é homologada (Filtro 4), o item DEVE ter resultado
                            if it.get('temResultado') is True:
                                num_item = it.get('numeroItem')
                                
                                # 3. Busca o Vencedor
                                url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens/{num_item}/resultados"
                                
                                try:
                                    r_win = requests.get(url_res, headers=HEADERS, timeout=10)
                                    if r_win.status_code == 200:
                                        resultados = r_win.json()
                                        if isinstance(resultados, dict): resultados = [resultados]

                                        for res in resultados:
                                            cnpj_forn = res.get('niFornecedor')
                                            nome_forn = res.get('nomeRazaoSocialFornecedor')
                                            
                                            # Tratamento permissivo
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
                                time.sleep(0.05)
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

print(f"\n✅ SUCESSO! {len(final)} registros salvos.")
