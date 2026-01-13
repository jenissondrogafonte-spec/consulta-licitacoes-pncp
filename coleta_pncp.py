import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os

# --- CONFIGURAÇÕES ---
# Busca dados de ONTEM (D-1) para garantir dia fechado
DATA_BUSCA = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
ARQUIVO_SAIDA = 'dados.json'

print(f"--- INICIANDO ROBÔ PNCP: {DATA_BUSCA} ---")

# Lista para acumular todos os itens ganhos do dia
todos_itens_ganhos = []

# 1. BUSCAR CONTRATAÇÕES (LICITAÇÕES) DO DIA
url_contratacoes = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
params = {
    "dataInicial": DATA_BUSCA,
    "dataFinal": DATA_BUSCA,
    "pagina": 1,
    "tamanhoPagina": 50  # Pega as 50 primeiras licitações do dia (para não estourar tempo do GitHub grátis)
}

try:
    print(f"Consultando licitações em: {url_contratacoes}")
    resp_contratacoes = requests.get(url_contratacoes, params=params)
    resp_contratacoes.raise_for_status()
    lista_contratacoes = resp_contratacoes.json().get('data', [])
    print(f"> Encontradas {len(lista_contratacoes)} licitações para verificar.")
except Exception as e:
    print(f"Erro fatal na busca inicial: {e}")
    lista_contratacoes = []

# 2. ENTRAR EM CADA LICITAÇÃO E PEGAR RESULTADOS
for i, compra in enumerate(lista_contratacoes):
    try:
        # Dados básicos da compra para vincular ao resultado
        orgao_cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
        ano_compra = compra.get('anoCompra')
        seq_compra = compra.get('sequencialCompra')
        num_licitacao = f"{seq_compra}/{ano_compra}"
        
        if not (orgao_cnpj and ano_compra and seq_compra):
            continue

        # Monta URL específica de RESULTADOS (Baseado no padrão da API PNCP)
        # Endpoint: /orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/resultados
        url_resultados = f"https://pncp.gov.br/api/pncp/v1/orgaos/{orgao_cnpj}/compras/{ano_compra}/{seq_compra}/itens/resultados"
        
        # Consulta
        print(f"  [{i+1}/{len(lista_contratacoes)}] Verificando resultados: {num_licitacao}...")
        resp_res = requests.get(url_resultados)
        
        if resp_res.status_code == 200:
            itens = resp_res.json()
            # Adiciona dados da licitação em cada item para facilitar o agrupamento
            for item in itens:
                # Só queremos quem GANHOU (Homologado)
                if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                    item['_num_licitacao'] = num_licitacao
                    item['_orgao_codigo'] = orgao_cnpj
                    todos_itens_ganhos.append(item)
        
        time.sleep(0.5) # Pausa respeitosa para não bloquear

    except Exception as e:
        print(f"  Erro ao processar compra {i}: {e}")

# 3. PROCESSAMENTO E SOMA (O GRANDE SEGREDO)
print(f"--- Processando {len(todos_itens_ganhos)} itens ganhos encontrados ---")

dados_finais = []

if todos_itens_ganhos:
    df = pd.DataFrame(todos_itens_ganhos)
    
    # Agrupamento Mágico: Junta tudo que é do mesmo Fornecedor na mesma Licitação
    # Campos chaves: CNPJ do Fornecedor, Nome do Fornecedor, Licitação e Órgão
    agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_num_licitacao', '_orgao_codigo']).agg({
        'numeroItem': 'count',           # Conta quantos itens levou
        'valorTotalHomologado': 'sum',   # SOMA o dinheiro ganho
        'dataResultado': 'first'         # Pega a data
    }).reset_index()

    # Formata para o JSON do site
    for _, row in agrupado.iterrows():
        dados_finais.append({
            "Portal": "PNCP",
            "Orgao_Codigo": row['_orgao_codigo'],
            "Num_Licitacao": row['_num_licitacao'],
            "Fornecedor": row['nomeRazaoSocialFornecedor'],
            "Itens_Ganhos": int(row['numeroItem']),
            "Total_Ganho_R$": float(row['valorTotalHomologado']),
            "Data_Homologacao": row['dataResultado'][:10] # Só a data YYYY-MM-DD
        })

# 4. SALVAR/ATUALIZAR ARQUIVO
# Lê o antigo se existir para não perder histórico
if os.path.exists(ARQUIVO_SAIDA):
    try:
        with open(ARQUIVO_SAIDA, 'r', encoding='utf-8') as f:
            historico = json.load(f)
    except:
        historico = []
else:
    historico = []

# Adiciona novos (em um sistema real, verificaria duplicatas aqui)
historico.extend(dados_finais)

with open(ARQUIVO_SAIDA, 'w', encoding='utf-8') as f:
    json.dump(historico, f, ensure_ascii=False, indent=4)

print("--- SUCESSO! DADOS REAIS SALVOS ---")
