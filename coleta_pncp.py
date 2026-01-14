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

# Datas: Se não informado no Actions, usa Outubro/2025 (Teste seguro)
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    d_ini = "20251001"
    d_fim = "20251005"

print(f"--- ROBÔ HÍBRIDO (STATUS + VENCEDORES): {d_ini} até {d_fim} ---")

ARQ_VENCEDORES = 'dados.json'
ARQ_STATUS = 'status.json'

lista_vencedores = []
lista_status = []
MAX_PAGINAS = 100 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while pagina <= MAX_PAGINAS:
        # 1. BUSCA A LISTA GERAL (SEM FILTRO DE SITUAÇÃO)
        # Removemos 'situacaoCompraId' para pegar Abertas, Suspensas, etc.
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
            if not licitacoes: break # Fim das páginas deste dia
            
            print(f"[P{pagina}]", end=" ", flush=True)

            for lic in licitacoes:
                # Dados Básicos
                cnpj_orgao = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}"
                
                # Dados para o Status
                objeto = lic.get('objetoCompra', 'Objeto não informado')
                situacao_nome = lic.get('situacaoCompraNome', 'Desconhecido')
                situacao_id = lic.get('situacaoCompraId') 

                # --- AÇÃO 1: SALVAR NO STATUS.JSON ---
                # Salva tudo, independente se tem vencedor ou não
                lista_status.append({
                    "Data": DATA_STR,
                    "UASG": uasg,
                    "Orgao": nome_orgao,
                    "Licitacao": id_licitacao,
                    "Objeto": objeto,
                    "Status": situacao_nome
                })

                # --- AÇÃO 2: SALVAR NO DADOS.JSON (SÓ SE TIVER RESULTADO) ---
                # Só gastamos tempo buscando vencedores se a licitação estiver
                # Homologada (4), Adjudicada (6) ou Encerrada (8)
                if situacao_id in [4, 6, 8]:
                    url_res_global = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/resultados"
                    
                    try:
                        r_glob = requests.get(url_res_global, headers=HEADERS, timeout=10)
                        if r_glob.status_code == 200:
                            resultados = r_glob.json()
                            if isinstance(resultados, dict): resultados = [resultados]

                            for res in resultados:
                                cnpj_forn = res.get('niFornecedor')
                                nome_forn = res.get('nomeRazaoSocialFornecedor')
                                
                                # Tratamento para nome vazio
                                if not nome_forn and cnpj_forn: 
                                    nome_forn = f"CNPJ {cnpj_forn}"
                                
                                valor = res.get('valorTotalHomologado')
                                if valor is None: valor = 0
                                
                                if cnpj_forn:
                                    lista_vencedores.append({
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

# --- FUNÇÃO PARA SALVAR E REMOVER DUPLICATAS ---
def salvar_arquivo_json(nome_arquivo, dados_novos):
    if not dados_novos:
        print(f"\n⚠️ Sem dados novos para {nome_arquivo}.")
        return

    historico = []
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            try: historico = json.load(f)
            except: historico = []
    
    historico.extend(dados_novos)
    
    # Remove duplicatas transformando em string JSON (hashable) e usando set
    # Isso garante que não teremos linhas repetidas se você rodar 2x
    final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]
    
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=4, ensure_ascii=False)
    
    print(f"💾 {nome_arquivo} atualizado! Total: {len(final)} registros.")

# --- FINALIZAÇÃO ---
print("\n--- RESUMO DA COLETA ---")

# 1. Salva Vencedores (Agrupando valores do mesmo fornecedor na mesma licitação)
if lista_vencedores:
    df = pd.DataFrame(lista_vencedores)
    # Soma valores e itens se a empresa ganhou mais de um lote na mesma licitação
    grp = df.groupby(['CNPJ', 'Fornecedor', 'Licitacao', 'Orgao', 'UASG', 'Data']).agg({'Itens': 'sum', 'Total': 'sum'}).reset_index()
    salvar_arquivo_json(ARQ_VENCEDORES, grp.to_dict(orient='records'))
else:
    print(f"⚠️ Nenhum vencedor encontrado no período.")

# 2. Salva Status (Removendo duplicatas antes de salvar para economizar espaço)
if lista_status:
    # Remove duplicatas imediatas da lista atual
    df_st = pd.DataFrame(lista_status).drop_duplicates(subset=['Licitacao', 'Status'])
    salvar_arquivo_json(ARQ_STATUS, df_st.to_dict(orient='records'))
else:
    print(f"⚠️ Nenhuma licitação (status) encontrada no período.")
