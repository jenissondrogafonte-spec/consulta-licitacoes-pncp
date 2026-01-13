import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os

# --- CONFIGURAÇÕES ---
# Pega dados de ONTEM (D-1)
DATA_BUSCA = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
ARQUIVO_SAIDA = 'dados.json'

print(f"--- INICIANDO ROBÔ TURBO: {DATA_BUSCA} ---")

todos_itens_ganhos = []

# ==============================================================================
# 1. BUSCAR TODAS AS CONTRATAÇÕES (PAGINAÇÃO AUTOMÁTICA)
# ==============================================================================
url_base_contratacoes = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
lista_contratacoes = []
pagina_atual = 1

while True:
    print(f"Baixando página {pagina_atual} de licitações...")
    params = {
        "dataInicial": DATA_BUSCA,
        "dataFinal": DATA_BUSCA,
        "pagina": pagina_atual,
        "tamanhoPagina": 50 # Pede de 50 em 50 para não travar
    }
    
    try:
        resp = requests.get(url_base_contratacoes, params=params)
        
        # Se der erro ou não tiver mais nada, para.
        if resp.status_code != 200:
            print(f"Fim das páginas ou erro: Status {resp.status_code}")
            break
            
        dados = resp.json().get('data', [])
        
        if not dados: # Lista vazia, acabou.
            print("Página vazia. Fim da coleta.")
            break
            
        lista_contratacoes.extend(dados)
        
        # Proteção de Loop Infinito (opcional: limitar a 20 páginas para teste)
        # if pagina_atual >= 20: break 
        
        pagina_atual += 1
        time.sleep(0.3) # Respira para não bloquear
        
    except Exception as e:
        print(f"Erro na página {pagina_atual}: {e}")
        break

print(f"> Total de licitações encontradas no dia: {len(lista_contratacoes)}")

# ==============================================================================
# 2. ENTRAR EM CADA LICITAÇÃO E PEGAR RESULTADOS
# ==============================================================================
# Limitador de segurança para o GitHub Actions gratuito (Processa max 300 para não estourar tempo)
# Se quiser processar tudo, remova o [:300]
lista_processamento = lista_contratacoes[:300] 

for i, compra in enumerate(lista_processamento):
    try:
        orgao_cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
        ano_compra = compra.get('anoCompra')
        seq_compra = compra.get('sequencialCompra')
        nome_orgao = compra.get('orgaoEntidade', {}).get('razaoSocial')
        
        if not (orgao_cnpj and ano_compra and seq_compra):
            continue

        num_licitacao = f"{seq_compra}/{ano_compra}"

        # Endpoint de Resultados
        url_resultados = f"https://pncp.gov.br/api/pncp/v1/orgaos/{orgao_cnpj}/compras/{ano_compra}/{seq_compra}/itens/resultados"
        
        resp_res = requests.get(url_resultados)
        
        if resp_res.status_code == 200:
            itens = resp_res.json()
            # Se vier só um dicionário (bug comum da API), transforma em lista
            if isinstance(itens, dict): itens = [itens]
            
            for item in itens:
                # Pega apenas HOMOLOGADOS (Vencedores reais)
                if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                    item['_num_licitacao'] = num_licitacao
                    item['_orgao_codigo'] = f"{orgao_cnpj} - {nome_orgao}"
                    # Data: tenta pegar do resultado, se não tiver, pega da licitação
                    data_res = item.get('dataResultado') or compra.get('dataPublicacaoPncp')
                    item['_data_homologacao'] = data_res[:10] if data_res else DATA_BUSCA
                    todos_itens_ganhos.append(item)
        
        if i % 10 == 0: print(f"Processando {i}...") # Log de progresso
        time.sleep(0.2)

    except Exception as e:
        pass # Segue o baile

# ==============================================================================
# 3. AGRUPAR E SALVAR
# ==============================================================================
print(f"--- Processando {len(todos_itens_ganhos)} itens ganhos ---")
dados_finais = []

if todos_itens_ganhos:
    df = pd.DataFrame(todos_itens_ganhos)
    
    # Agrupa por Fornecedor + Licitação + Órgão
    agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_num_licitacao', '_orgao_codigo', '_data_homologacao']).agg({
        'numeroItem': 'count',
        'valorTotalHomologado': 'sum'
    }).reset_index()

    for _, row in agrupado.iterrows():
        dados_finais.append({
            "Data_Homologacao": row['_data_homologacao'],
            "Orgao_Codigo": row['_orgao_codigo'],
            "Num_Licitacao": row['_num_licitacao'],
            "Fornecedor": row['nomeRazaoSocialFornecedor'],
            "CNPJ_Fornecedor": row['niFornecedor'],
            "Itens_Ganhos": int(row['numeroItem']),
            "Total_Ganho_R$": float(row['valorTotalHomologado'])
        })

# Carrega histórico anterior
if os.path.exists(ARQUIVO_SAIDA):
    try:
        with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
            historico = json.load(f)
    except: historico = []
else:
    historico = []

historico.extend(dados_finais)

# Remove duplicatas exatas
historico_unico = list({json.dumps(i, sort_keys=True) for i in historico})
historico_limpo = [json.loads(i) for i in historico_unico]

# Salva
with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(historico_limpo, f, ensure_ascii=False, indent=4)

print("--- SUCESSO! ---")
