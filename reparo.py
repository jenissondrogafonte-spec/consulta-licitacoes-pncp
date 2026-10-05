import json
import os
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados.json'

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def main():
    if not os.path.exists(ARQ_DADOS):
        print("❌ Ficheiro dados.json não encontrado.")
        return

    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        banco = json.load(f)

    pendentes = []
    for item in banco:
        if not item.get('UASG') or not item.get('DataFimPropostas'):
            pendentes.append(item.get('Licitacao'))
            if len(pendentes) == 2:  # Vamos testar só com 2 para debugar rápido
                break

    print(f"🕵️ INICIANDO ESPIÃO PARA OS IDs: {pendentes}\n")

    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False

    for lic_id in pendentes:
        cnpj_org = lic_id[:14]
        seq = int(lic_id[14:-4])
        ano = lic_id[-4:]

        print(f"\n{'='*50}")
        print(f"🔎 TESTANDO PROCESSO: {lic_id}")
        print(f"{'='*50}")
        
        # Teste 1: Rota de Consulta (Nova Arquitetura)
        url_nova = f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj_org}/{ano}/{seq}"
        print(f"\n🌐 TENTATIVA 1 (Rota Nova):\n{url_nova}")
        try:
            r_nova = session.get(url_nova, timeout=10)
            print(f"Status Code: {r_nova.status_code}")
            if r_nova.status_code == 200:
                print("Resposta JSON:", json.dumps(r_nova.json(), indent=2, ensure_ascii=False)[:1000], "\n...[cortado]")
            else:
                print("Texto de Erro:", r_nova.text[:500])
        except Exception as e:
            print(f"Erro de Conexão: {e}")

        # Teste 2: Rota Interna PNCP (Antiga Arquitetura)
        url_antiga = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}"
        print(f"\n🌐 TENTATIVA 2 (Rota Antiga):\n{url_antiga}")
        try:
            r_antiga = session.get(url_antiga, timeout=10)
            print(f"Status Code: {r_antiga.status_code}")
            if r_antiga.status_code == 200:
                print("Resposta JSON:", json.dumps(r_antiga.json(), indent=2, ensure_ascii=False)[:1000], "\n...[cortado]")
            else:
                print("Texto de Erro:", r_antiga.text[:500])
        except Exception as e:
            print(f"Erro de Conexão: {e}")

if __name__ == "__main__":
    main()
