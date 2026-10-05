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

    # Usando o mesmo processo que descobrimos na sua última execução
    cnpj_org = "80881915000192"
    ano = "2026"
    seq = "44"

    print(f"🕵️ ESPIÃO 3 - Forçando o servidor a revelar a nova rota de itens\n")

    # Batendo na rota ANTIGA para ver a mensagem de redirecionamento (301)
    url_itens_antiga = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=50"
    
    print(f"🌐 ROTA ANTIGA:\n{url_itens_antiga}")
    try:
        r = session.get(url_itens_antiga, timeout=10)
        print(f"Status Code: {r.status_code}")
        print(f"Resposta JSON:\n{r.text[:600]}")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()
