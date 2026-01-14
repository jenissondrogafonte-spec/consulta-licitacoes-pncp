import requests
import json
import sys

# --- DADOS DO ALVO (PREFEITURA DE PALMEIRA/SC) ---
CNPJ = "01610566000106"
ANO = "2025"
SEQ = "68"

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

print(f"--- TESTE SNIPER: Alvo {SEQ}/{ANO} - CNPJ {CNPJ} ---")

# 1. Busca os itens da licitação
url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{CNPJ}/compras/{ANO}/{SEQ}/itens"
print(f"1. Acessando lista de itens: {url_itens}")

try:
    resp = requests.get(url_itens, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        print(f"❌ Erro ao acessar itens: {resp.status_code}")
        sys.exit()
        
    itens = resp.json()
    print(f"✅ Itens encontrados: {len(itens)}")
    
    # Pega o primeiro item
    item = itens[0]
    num_item = item.get('numeroItem')
    print(f"   Item #{num_item}: {item.get('descricao')}")
    print(f"   Status 'temResultado': {item.get('temResultado')}")

    if not item.get('temResultado'):
        print("❌ O item diz que NÃO tem resultado. Fim do teste.")
        sys.exit()

    # 2. TENTATIVA A: Endpoint Padrão de Resultado do Item
    # Padrão: /itens/{numeroItem}/resultados
    url_tentativa_a = f"https://pncp.gov.br/api/pncp/v1/orgaos/{CNPJ}/compras/{ANO}/{SEQ}/itens/{num_item}/resultados"
    print(f"\n2. Tentativa A (Endpoint Padrão): {url_tentativa_a}")
    
    r_a = requests.get(url_tentativa_a, headers=HEADERS)
    print(f"   Status Code: {r_a.status_code}")
    print(f"   Conteúdo: {r_a.text[:200]}") # Imprime os primeiros 200 caracteres

    # 3. TENTATIVA B: Endpoint Alternativo (Resultado Geral da Compra)
    # Às vezes o resultado não está no item, mas na compra global
    url_tentativa_b = f"https://pncp.gov.br/api/pncp/v1/orgaos/{CNPJ}/compras/{ANO}/{SEQ}/resultados"
    print(f"\n3. Tentativa B (Endpoint Global): {url_tentativa_b}")
    
    r_b = requests.get(url_tentativa_b, headers=HEADERS)
    print(f"   Status Code: {r_b.status_code}")
    
    if r_b.status_code == 200:
        lista_res = r_b.json()
        print(f"   Resultados globais encontrados: {len(lista_res)}")
        if len(lista_res) > 0:
            print("   DADOS DO PRIMEIRO RESULTADO GLOBAL:")
            print(json.dumps(lista_res[0], indent=4, ensure_ascii=False))

except Exception as e:
    print(f"❌ Erro crítico no script: {e}")
