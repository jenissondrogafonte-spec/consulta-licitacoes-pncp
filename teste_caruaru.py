import requests
import urllib3
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO EXTRAÍDA DO SEU TEXTO ---
CNPJ_ORGAO = "10572048001523"  # CNPJ do Fundo Mun. Saúde Caruaru
ANO_COMPRA = "2025"
SEQUENCIAL = "3"               # Sequencial extraído do ID PNCP
CNPJ_ALVO = "08778201000126"   # Seu CNPJ

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

def criar_sessao():
    session = requests.Session()
    session.verify = False
    # Tenta reconectar até 5 vezes se o governo der erro 500/502/503
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def analisar_item(session, it):
    num = it['numeroItem']
    desc = it.get('descricao', 'Sem descrição')
    
    # Se o campo temResultado for falso, nem perde tempo (mas avisa)
    if not it.get('temResultado'):
        return f"⚪ Item {num}: Sem resultado homologado ainda."
    
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{CNPJ_ORGAO}/compras/{ANO_COMPRA}/{SEQUENCIAL}/itens/{num}/resultados"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            vends = resp.json()
            if isinstance(vends, dict): vends = [vends]
            
            vencedor_nome = "N/A"
            for v in vends:
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                vencedor_nome = v.get('nomeRazaoSocialFornecedor')
                
                # VERIFICA SE É VOCÊ
                if CNPJ_ALVO in ni:
                    val_unit = v.get('valorUnitarioHomologado', 0)
                    qtd = v.get('quantidadeHomologada', 0)
                    total = v.get('valorTotalHomologado', 0)
                    return f"✅ VENCEU! Item {num} ({desc}) | Qtd: {qtd} | R$ {val_unit} (Total: R$ {total})"
            
            return f"❌ Perdeu Item {num}. Vencedor: {vencedor_nome}"
        else:
            return f"⚠️ Item {num}: Erro na API (Status {resp.status_code})"
    except Exception as e:
        return f"💀 Item {num}: Erro de Conexão ({str(e)})"

# --- EXECUÇÃO ---
session = criar_sessao()
print(f"🔍 Auditando CARUARU-PE (Edital 90000/2025)...")
print(f"🎯 Alvo: {CNPJ_ALVO}\n")

todos_itens = []
pagina = 1

# 1. BAJA A LISTA DE ITENS (COM PAGINAÇÃO CORRIGIDA)
while True:
    print(f"📥 Baixando página {pagina} de itens...", end=" ")
    url_lista = f"https://pncp.gov.br/api/pncp/v1/orgaos/{CNPJ_ORGAO}/compras/{ANO_COMPRA}/{SEQUENCIAL}/itens?pagina={pagina}&tamanhoPagina=1000"
    
    try:
        r = session.get(url_lista, headers=HEADERS)
        if r.status_code != 200:
            print(f"Erro {r.status_code}")
            break
            
        dados = r.json()
        if not dados: break 
        
        todos_itens.extend(dados)
        print(f"Ok (+{len(dados)} itens).")
        
        if len(dados) < 1000: break
        pagina += 1
    except Exception as e:
        print(f"Erro fatal: {e}")
        break

print(f"\n📦 Total de itens nesta licitação: {len(todos_itens)}")
print("-" * 60)

# 2. ANALISA QUEM GANHOU (PARALELO)
if todos_itens:
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analisar_item, session, it): it for it in todos_itens}
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            # Só imprime se for Vitória, Erro ou Perda (Opcional: tire o filtro para ver tudo)
            if "VENCEU" in res or "💀" in res:
                print(res)
else:
    print("Nenhum item encontrado. Verifique se o Sequencial/Ano estão corretos.")
