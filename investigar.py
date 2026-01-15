import requests
import json

# CONFIGURAÇÃO DA CAÇA
UASG_ALVO = "926809"       # Secretaria de Saúde de Caruaru
NUMERO_ALVO = "90000"      # O número que queremos achar

print(f"🕵️  Investigando a UASG {UASG_ALVO} em busca do Edital {NUMERO_ALVO}...")

# Vamos buscar o ano todo de 2025
url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
params = {
    "dataInicial": "20250101",
    "dataFinal": "20251231",
    "codigoUnidadeCompradora": UASG_ALVO, # FILTRO DE OURO: Só essa UASG
    "pagina": 1,
    "tamanhoPagina": 50
}

encontrado = False

# Varre até 20 páginas (suficiente para uma UASG em um ano)
for i in range(1, 21):
    print(f"   ...Lendo página {i}")
    params["pagina"] = i
    
    try:
        resp = requests.get(url, params=params)
        lista = resp.json().get('data', [])
        
        if not lista: break # Acabaram as licitações
        
        for lic in lista:
            seq = str(lic.get('sequencialCompra', ''))
            ano = str(lic.get('anoCompra', ''))
            dt_pub = lic.get('dataPublicacaoPncp', '')[:8] # YYYYMMDD
            modalidade = lic.get('modalidadeAmparoNome', '')
            obj = lic.get('objetoCompra', '')
            
            # Verifica se o sequencial é 90000
            if seq == NUMERO_ALVO:
                print("\n" + "="*50)
                print("✅ ENCONTREI!")
                print(f"📜 Processo: {seq}/{ano}")
                print(f"📅 DATA DE PUBLICAÇÃO: {dt_pub} <--- (USE ESSA DATA NO ROBÔ)")
                print(f"⚖️  Modalidade: {modalidade}")
                print(f"📦 Objeto: {obj}")
                print("="*50 + "\n")
                encontrado = True
                break
        
        if encontrado: break
            
    except Exception as e:
        print(f"Erro de conexão: {e}")
        break

if not encontrado:
    print("\n❌ Não encontrei com o número exato 90000.")
    print("Dica: Tente rodar o robô principal pegando o ano todo (20250101 a 20251231).")
