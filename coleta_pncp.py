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
    d_ini, d_fim = env_inicio, env_fim
else:
    # Padrão: Um intervalo curto de dias úteis recentes
    hoje = datetime.now()
    d_ini = (hoje - timedelta(days=5)).strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

print(f"--- ROBÔ DE VARREDURA PROFUNDA: {d_ini} até {d_fim} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens = []

# MÁXIMO DE PÁGINAS PARA BUSCAR POR DIA (Aumente se necessário)
MAX_PAGINAS = 5 

# Loop por datas
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Analisando dia {DATA_STR}...", end=" ")
    
    pagina = 1
    encontrados_no_dia = 0
    
    while pagina <= MAX_PAGINAS:
        # URL de Publicação (A mais estável)
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
            
            if resp.status_code == 200:
                licitacoes = resp.json().get('data', [])
                if not licitacoes:
                    break # Acabaram as licitações deste dia
                
                print(f"[Pág {pagina}]", end=" ", flush=True)

                for lic in licitacoes:
                    # OTIMIZAÇÃO: Pula licitações que sabemos que não tem resultado
                    # ID 1 = Divulgação, 2 = Recebendo Proposta.
                    # Queremos ID 4 (Homologada), 8 (Encerrada), etc.
                    situacao_id = lic.get('situacaoCompraId')
                    
                    # Se for status inicial, nem perde tempo consultando
                    if situacao_id in [1, 2, 3]: 
                        continue

                    cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                    nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')

                    # Busca Vencedores
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                    
                    try:
                        r_res = requests.get(url_res, headers=HEADERS, timeout=10)
                        if r_res.status_code == 200:
                            itens = r_res.json()
                            if isinstance(itens, dict): itens = [itens]
                            
                            for it in itens:
                                fornecedor = it.get('nomeRazaoSocialFornecedor')
                                valor = it.get('valorTotalHomologado', 0)
                                
                                if fornecedor and valor > 0:
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
                                    encontrados_no_dia += 1
                    except:
                        pass
                
                pagina += 1
                time.sleep(0.5) # Respeita o servidor entre páginas
            else:
                break # Erro na página, vai para o próximo dia
        except:
            break

    print(f"-> {encontrados_no_dia} itens coletados.")
    data_atual += timedelta(days=1)

# --- SALVAMENTO ---
if not todos_itens:
    print("\n⚠️ Nenhum dado encontrado. O robô varreu as páginas mas não achou homologações.")
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

print(f"\n💾 SUCESSO! Banco de dados atualizado com {len(final)} registros.")
