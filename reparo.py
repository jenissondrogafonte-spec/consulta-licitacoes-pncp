import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False

    # O CNPJ e Sequencial exato da sua imagem (Consórcio de Viçosa)
    url = "https://pncp.gov.br/api/consulta/v1/orgaos/02326365000136/compras/2026/28"

    print(f"🕵️ BASCULHANDO O JSON EXATO DESTE PROCESSO:\n{url}\n")

    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            dados = r.json()
            print("=== ESTRUTURA DE DADOS RECEBIDA ===")
            print(json.dumps(dados, indent=4, ensure_ascii=False))
        else:
            print(f"Erro {r.status_code}: {r.text}")
    except Exception as e:
        print(f"Falha de conexão: {e}")

if __name__ == "__main__":
    main()
