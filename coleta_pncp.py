import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os

# --- LEITURA DAS DATAS (MÁQUINA DO TEMPO) ---
# Se o GitHub mandar datas manuais, usa elas. Se não, usa "Ontem".
env_inicio = os.getenv('DATA_INICIAL')
env_fim = os.getenv('DATA_FINAL')

if env_inicio and env_fim:
    # Modo Manual: O usuário escolheu as datas
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
    print(f"--- MODO MANUAL ATIVADO: De {env_inicio} até {env_fim} ---")
else:
    # Modo Automático: Pega apenas Ontem
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem
    print(f"--- MODO AUTOMÁTICO: Buscando apenas {data_ontem.strftime('%Y%m%d')} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens_ganhos = []

# Loop para percorrer CADA DIA do período escolhido
while data_atual <= data_limite:
    DATA_BUSCA = data_atual.strftime('%Y%m%d')
    print(f"\n>>> PROCESSANDO DATA: {DATA_BUSCA} <<<")

    # ==============================================================================
    # 1. BUSCAR LICITAÇÕES (PAGINAÇÃO)
    # ==============================================================================
    url_base = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pagina = 1
    
    while True:
        params = {"dataInicial": DATA_BUSCA, "dataFinal": DATA_BUSCA, "pagina": pagina, "tamanhoPagina": 50}
        try:
            resp = requests.get(url_base, params=params)
            if resp.status_code != 200: break
            dados = resp.json().get('data', [])
            if not dados: break
            
            # 2. ENTRAR NOS RESULTADOS
            # Processa apenas as primeiras 100 licitações por página para economizar tempo
            # Se quiser TUDO, remova o [:100]
            for compra in dados[:100]: 
                try:
                    cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
                    ano = compra.get('anoCompra')
                    seq = compra.get('sequencialCompra')
                    nome_orgao = compra.get('orgaoEntidade', {}).get('razaoSocial')
                    
                    if not (cnpj and ano and seq): continue
                    
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                    resp_res = requests.get(url_res)
                    
                    if resp_res.status_code == 200:
                        itens = resp_res.json()
                        if isinstance(itens, dict): itens = [itens]
                        
                        for item in itens:
                            if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                                item['_data_ref'] = DATA_BUSCA
                                item['_orgao'] = f"{cnpj} - {nome_orgao}"
                                item['_licitacao'] = f"{seq}/{ano}"
                                todos_itens_ganhos.append(item)
                    time.sleep(0.1) # Pequena pausa
                except: pass
            
            print(f"  - Pág {pagina} ok...")
            pagina += 1
            # Limite de segurança: se tiver mais de 50 páginas num dia, para (evita travar)
            if pagina > 50: break 
            
        except: break

    # Avança para o próximo dia
    data_atual += timedelta(days=1)

# ==============================================================================
# 3. SALVAR TUDO
# ==============================================================================
print(f"\n--- Salvando {len(todos_itens_ganhos)} novos registros ---")

novos_dados = []
if todos_itens_ganhos:
    df = pd.DataFrame(todos_itens_ganhos)
    # Agrupa
    agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_licitacao', '_orgao', '_data_ref']).agg({
        'numeroItem': 'count', 'valorTotalHomologado': 'sum'
    }).reset_index()

    for _, row in agrupado.iterrows():
        novos_dados.append({
            "Data_Homologacao": row['_data_ref'], # Formato YYYYMMDD
            "Orgao_Codigo": row['_orgao'],
            "Num_Licitacao": row['_licitacao'],
            "Fornecedor": row['nomeRazaoSocialFornecedor'],
            "Total_Ganho_R$": float(row['valorTotalHomologado']),
            "Itens_Ganhos": int(row['numeroItem'])
        })

# Mescla com histórico
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

print("--- CONCLUÍDO ---")
