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

# Se não informado, pega os últimos 3 dias
if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    hoje = datetime.now()
    d_ini = (hoje - timedelta(days=3)).strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

print(f"--- ROBÔ DE VARREDURA (LISTA DE ITENS): {d_ini} até {d_fim} ---")

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
        # 1. Busca Licitações (Publicações)
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            licitacoes = resp.json().get('data', [])
            if not licitacoes: break
            
            print(f"[P{pagina}]", end=" ", flush=True)

            for lic in licitacoes:
                # Se a licitação estiver "Aberta" ou "Em disputa", ignoramos
                # IDs comuns: 4=Homologada, 6=Adjudicada, 8=Encerrada
                if lic.get('situacaoCompraId') in [1, 2, 3]: 
                    continue

                cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')

                # 2. MUDANÇA: Busca na lista geral de ITENS (não na de 'resultados')
                url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
                
                try:
                    r_it = requests.get(url_itens, headers=HEADERS, timeout=10)
                    if r_it.status_code == 200:
                        lista_itens = r_it.json()
                        if isinstance(lista_itens, dict): lista_itens = [lista_itens]

                        for it in lista_itens:
                            # O SEGREDO: Verificamos se tem fornecedor vinculado ao item
                            fornecedor = it.get('nomeRazaoSocialFornecedor')
                            valor = it.get('valorTotalHomologado', 0)
                            
                            # Se não tiver valor homologado, tenta pegar o valor unitário estimado x qtd
                            # (Mas para seu caso, focamos em quem GANHOU)
                            if fornecedor:
                                if valor == 0: valor = it.get('valorTotalItem', 0)

                                todos_itens.append({
                                    "Data": DATA_STR,
                                    "UASG": uasg,
                                    "Orgao": nome_orgao,
                                    "Licitacao": f"{uasg}{str(seq).zfill(5)}{ano}",
                                    "Fornecedor": fornecedor,
                                    "CNPJ": it.get('niFornecedor', ''),
                                    "Total": float(valor),
                                    "Itens": 1
                                })
                except: pass
            
            pagina += 1
            time.sleep(0.5)

        except: break
    
    data_atual += timedelta(days=1)

# --- FIM E SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum dado encontrado. Tente uma data mais antiga (ex: Outubro/2025).")
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
