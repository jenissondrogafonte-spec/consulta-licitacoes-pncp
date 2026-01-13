import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import os

# --- LEITURA DE ENTRADAS ---
env_inicio = os.getenv('DATA_INICIAL')
env_fim = os.getenv('DATA_FINAL')

if env_inicio and env_fim:
    # Remove barras se o usuário digitou errado (ex: 2025/01/01 vira 20250101)
    env_inicio = env_inicio.replace('/', '').replace('-', '')
    env_fim = env_fim.replace('/', '').replace('-', '')
    
    data_atual = datetime.strptime(env_inicio, '%Y%m%d')
    data_limite = datetime.strptime(env_fim, '%Y%m%d')
    print(f"--- MODO MANUAL: De {env_inicio} até {env_fim} ---")
else:
    data_ontem = datetime.now() - timedelta(days=1)
    data_atual = data_ontem
    data_limite = data_ontem
    print(f"--- MODO AUTOMÁTICO (ONTEM): {data_ontem.strftime('%Y%m%d')} ---")

ARQUIVO_SAIDA = 'dados.json'
todos_itens_ganhos = []

# --- LOOP POR DIA ---
while data_atual <= data_limite:
    DATA_BUSCA = data_atual.strftime('%Y%m%d')
    print(f"\n==========================================")
    print(f"🔎 INVESTIGANDO DIA: {DATA_BUSCA}")
    print(f"==========================================")

    url_base = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    pagina = 1
    total_encontrado_dia = 0
    
    while True:
        print(f"  > Baixando página {pagina} de contratações...")
        params = {"dataInicial": DATA_BUSCA, "dataFinal": DATA_BUSCA, "pagina": pagina, "tamanhoPagina": 50}
        
        try:
            resp = requests.get(url_base, params=params)
            if resp.status_code != 200:
                print(f"    ❌ Erro na API (Status {resp.status_code})")
                break
            
            dados = resp.json().get('data', [])
            if not dados:
                print("    ⚠️ Página vazia (fim do dia ou sem dados).")
                break
            
            print(f"    ✅ Página {pagina}: Encontrei {len(dados)} licitações. Verificando vencedores...")
            
            # Entra em cada licitação
            for i, compra in enumerate(dados): 
                try:
                    cnpj = compra.get('orgaoEntidade', {}).get('cnpj')
                    ano = compra.get('anoCompra')
                    seq = compra.get('sequencialCompra')
                    
                    # Validação básica
                    if not (cnpj and ano and seq): continue
                    
                    # URL de Resultados
                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/resultados"
                    resp_res = requests.get(url_res)
                    
                    if resp_res.status_code == 200:
                        itens = resp_res.json()
                        if isinstance(itens, dict): itens = [itens] # Corrige bug da API se vier só 1
                        
                        count_homologados = 0
                        for item in itens:
                            if item.get('situacaoCompraItemResultadoNome') == 'Homologado':
                                # Captura os dados
                                item['_data_ref'] = DATA_BUSCA
                                item['_orgao'] = f"{compra.get('unidadeOrgao', {}).get('codigoUnidade', cnpj)} - {compra.get('orgaoEntidade', {}).get('razaoSocial')}"
                                item['_licitacao'] = f"{seq}/{ano}"
                                item['_processo'] = compra.get('processo', 'N/I')
                                todos_itens_ganhos.append(item)
                                count_homologados += 1
                        
                        if count_homologados > 0:
                            total_encontrado_dia += count_homologados
                            # Feedback visual simples (um ponto por sucesso)
                            print(".", end="", flush=True) 

                    time.sleep(0.1) # Pausa leve
                except Exception as e:
                    print(f"x", end="", flush=True)

            print(f"\n    (Fim da pág {pagina})")
            pagina += 1
            if pagina > 20: # Trava de segurança para não rodar infinitamente no teste
                print("    🛑 Parando na página 20 por segurança.")
                break 
            
        except Exception as e:
            print(f"    ❌ Erro fatal na conexão: {e}")
            break

    print(f"\nRESUMO DO DIA {DATA_BUSCA}: {total_encontrado_dia} itens ganhos coletados.")
    data_atual += timedelta(days=1)

# --- SALVAR ---
print(f"\n\n📊 TOTAL GERAL COLETADO: {len(todos_itens_ganhos)}")

if len(todos_itens_ganhos) == 0:
    print("❌ NENHUM DADO FOI SALVO PORQUE A LISTA ESTÁ VAZIA.")
    # Força erro para você ver no GitHub Actions
    exit(1) 

# Processamento Final
novos_dados = []
df = pd.DataFrame(todos_itens_ganhos)

# Agrupa
agrupado = df.groupby(['niFornecedor', 'nomeRazaoSocialFornecedor', '_licitacao', '_processo', '_orgao', '_data_ref']).agg({
    'numeroItem': 'count', 'valorTotalHomologado': 'sum'
}).reset_index()

for _, row in agrupado.iterrows():
    novos_dados.append({
        "Data_Homologacao": row['_data_ref'],
        "Orgao_Codigo": row['_orgao'],
        "Num_Licitacao": row['_licitacao'],
        "Num_Processo": row['_processo'],
        "Fornecedor": row['nomeRazaoSocialFornecedor'],
        "CNPJ_Fornecedor": row['niFornecedor'],
        "Total_Ganho_R$": float(row['valorTotalHomologado']),
        "Itens_Ganhos": int(row['numeroItem'])
    })

# Carrega e Salva
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

print(f"✅ SUCESSO! Arquivo salvo com {len(final)} registros totais.")
