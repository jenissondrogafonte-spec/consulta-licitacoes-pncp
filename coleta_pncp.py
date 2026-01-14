import requests
import json
import os
import sys

# --- CONFIGURAÇÃO DE DIAGNÓSTICO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Vamos pegar um dia fixo onde certamente houve pregão
DATA_FIXA = "20251001" 

print(f"--- MODO DIAGNÓSTICO: Analisando estrutura da API em {DATA_FIXA} ---")

# 1. Pega as licitações do dia
url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
params = {
    "dataInicial": DATA_FIXA,
    "dataFinal": DATA_FIXA,
    "codigoModalidadeContratacao": "6", # Pregão
    "pagina": 1,
    "tamanhoPagina": 10
}

try:
    print("1. Buscando licitações...")
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    licitacoes = resp.json().get('data', [])
    
    if not licitacoes:
        print("❌ Nenhuma licitação encontrada no endpoint de publicação.")
        sys.exit()
    
    print(f"✅ Encontradas {len(licitacoes)} licitações.")
    
    # 2. Pega a primeira licitação válida e entra nos itens
    for lic in licitacoes:
        cnpj = lic.get('orgaoEntidade', {}).get('cnpj')
        ano = lic.get('anoCompra')
        seq = lic.get('sequencialCompra')
        
        print(f"\n🔍 Inspecionando Licitação: {seq}/{ano} (CNPJ: {cnpj})")
        
        # URL da Lista de Itens
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
        
        r_it = requests.get(url_itens, headers=HEADERS, timeout=20)
        
        if r_it.status_code == 200:
            itens = r_it.json()
            print(f"   📦 Endpoint de itens respondeu com {len(itens)} itens.")
            
            if len(itens) > 0:
                print("\n🚨🚨🚨 ATENÇÃO: ABAIXO ESTÁ A ESTRUTURA REAL DO ITEM 🚨🚨🚨")
                primeiro_item = itens[0]
                
                # Imprime o JSON formatado para lermos
                print(json.dumps(primeiro_item, indent=4, ensure_ascii=False))
                
                print("\n---------------------------------------------------")
                print("Verifique no log acima: Onde está o nome do fornecedor?")
                print("Procure por campos como: 'nomeRazaoSocialFornecedor', 'fornecedor', 'resultado', etc.")
                sys.exit(0) # Para o script aqui para a gente analisar
        else:
            print(f"   ❌ Erro ao acessar itens: {r_it.status_code}")

except Exception as e:
    print(f"❌ Erro crítico: {e}")
