import requests
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False

    data_teste = "20260701"
    print(f"🕵️ ESPIÃO DE COLETA - Testando o dia {data_teste}\n")

    # 1. Buscar apenas 1 processo deste dia para o teste
    url_busca = f"https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?dataInicial={data_teste}&dataFinal={data_teste}&codigoModalidadeContratacao=6&pagina=1&tamanhoPagina=1"
    
    try:
        r_busca = session.get(url_busca, timeout=10)
        if r_busca.status_code != 200:
            print(f"❌ Erro na busca inicial: {r_busca.status_code} - {r_busca.text}")
            return
            
        dados = r_busca.json()
        lics = dados.get('data', [])
        if not lics:
            print("Nenhum processo encontrado neste dia.")
            return
            
        lic = lics[0]
        cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
        ano = lic.get('anoCompra')
        seq = lic.get('sequencialCompra')
        
        print(f"✅ Processo encontrado para teste: CNPJ {cnpj_org} | Ano {ano} | Seq {seq}")
        print("="*60)

        # 2. Testar Endpoint de Itens (A rota que está HOJE no coleta_pncp.py)
        url_itens_atual = f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj_org}/{ano}/{seq}/itens?pagina=1&tamanhoPagina=5"
        print(f"\n🌐 TENTATIVA 1 (Como o robô está fazendo hoje):\n{url_itens_atual}")
        r1 = session.get(url_itens_atual, timeout=10)
        print(f"Status Code: {r1.status_code}")
        print(f"Resposta: {r1.text[:300]}")

        # 3. Testar Endpoint de Itens (A rota corrigida, baseada no que aprendemos no reparo)
        url_itens_nova = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=5"
        print(f"\n🌐 TENTATIVA 2 (Nova rota provável):\n{url_itens_nova}")
        r2 = session.get(url_itens_nova, timeout=10)
        print(f"Status Code: {r2.status_code}")
        print(f"Resposta: {r2.text[:300]}")

    except Exception as e:
        print(f"Erro de execução: {e}")

if __name__ == "__main__":
    main()
