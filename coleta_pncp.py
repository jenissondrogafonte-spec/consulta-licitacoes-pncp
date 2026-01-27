import requests
import json
from datetime import datetime, timedelta
import os
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
MAX_WORKERS = 20               # Processamento paralelo (Turbo)
ARQ_DADOS = 'dados.json'
DIAS_RETROATIVOS = 365         # Quantos dias para trás ele vai buscar

# Desativa avisos de SSL (necessário para o site do governo)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# -------------------------------------------------
# 1. MOTOR DE CONEXÃO E ARQUIVOS
# -------------------------------------------------
def criar_sessao():
    """Cria uma sessão HTTP robusta com reconexão automática."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    # Configura retries para casos de falha momentânea do site
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def carregar_banco():
    """Lê o arquivo JSON atual para não perder o histórico antigo."""
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if not conteudo: return {}
                dados = json.loads(conteudo)
                # Cria um dicionário usando ID único para evitar duplicidade
                return {f"{i['Licitacao']}-{i['Item']}": i for i in dados}
        except Exception as e:
            print(f"⚠️ Aviso: Erro ao ler banco ({e}). Iniciando novo.")
    return {}

def salvar_estado(banco):
    """Grava os dados no disco."""
    lista_final = list(banco.values())
    # Ordena por data (mais recente primeiro)
    lista_final.sort(key=lambda x: x.get('DataResult', ''), reverse=True)
    
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    print(f" 💾 [Total salvo: {len(lista_final)} registros]", end="", flush=True)

# -------------------------------------------------
# 2. WORKER: PROCESSA UM ÚNICO ITEM
# -------------------------------------------------
def processar_item_individual(session, it, cnpj_org, ano, seq):
    """Verifica se a DROGAFONTE ganhou este item específico."""
    if not it.get('temResultado'):
        return None

    numero_item = it.get('numeroItem')
    # URL para pegar o resultado específico do item
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{numero_item}/resultados"
    
    try:
        r_v = session.get(url_res, timeout=20)
        if r_v.status_code == 200:
            vends = r_v.json()
            if isinstance(vends, dict): vends = [vends]
            
            for v in vends:
                # Limpa formatação do CNPJ para comparar
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                
                if CNPJ_ALVO in ni:
                    return {
                        "Item": numero_item,
                        "Descricao": it.get('descricao', 'Sem descrição'),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu"
                    }
    except:
        pass
    return None

# -------------------------------------------------
# 3. LÓGICA DE VARREDURA (LOOP DIÁRIO)
# -------------------------------------------------
def processar_dia(session, banco_total, data_atual):
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    pagina_edital = 1
    
    while True:
        # Busca todas as licitações onde o CNPJ participou naquele dia
        url_base = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina_edital, 
            "tamanhoPagina": 50, 
            "niFornecedor": CNPJ_ALVO
        }

        try:
            resp = session.get(url_base, params=params, timeout=30)
            if resp.status_code != 200: break
            
            json_resp = resp.json()
            lics = json_resp.get('data', [])
            
            # Se não tem nada nesta página, sai do loop de paginação
            if not lics: 
                if pagina_edital == 1: print("Sem registros.", end="")
                break

            # Itera sobre cada licitação encontrada
            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                edital_oficial = f"{lic.get('numeroCompra')}/{ano}"
                
                # --- PAGINAÇÃO INTERNA DOS ITENS DA LICITAÇÃO ---
                todos_itens_api = []
                pag_item = 1
                while True:
                    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina={pag_item}&tamanhoPagina=1000"
                    try:
                        r_it = session.get(url_itens, timeout=20)
                        if r_it.status_code == 200:
                            lote = r_it.json()
                            if not lote: break
                            todos_itens_api.extend(lote)
                            if len(lote) < 1000: break
                            pag_item += 1
                        else: break
                    except: break
                
                if not todos_itens_api: continue

                # --- PROCESSAMENTO PARALELO (SPEED TURBO) ---
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in todos_itens_api]
                    
                    for future in concurrent.futures.as_completed(futures):
                        res = future.result()
                        if res:
                            chave_unica = f"{id_lic}-{res['Item']}"
                            # Salva/Atualiza no dicionário
                            banco_total[chave_unica] = {
                                "DataPublicacao": DATA_STR,
                                "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                "UASG": uasg,
                                "Edital": edital_oficial,
                                "Licitacao": id_lic,
                                "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                                **res 
                            }
                            print("✅", end="", flush=True)

            # Verifica se tem mais páginas de editais no mesmo dia
            if pagina_edital >= json_resp.get('totalPaginas', 1): break
            pagina_edital += 1

        except Exception as e:
            print(f" (Erro: {e})", end="")
            break

# -------------------------------------------------
# 4. EXECUÇÃO PRINCIPAL
# -------------------------------------------------
def main():
    print(f"--- 🚀 INICIANDO VARREDURA (Últimos {DIAS_RETROATIVOS} dias) ---")
    
    session = criar_sessao()
    banco_total = carregar_banco()
    
    data_final = datetime.now()
    data_inicial = data_final - timedelta(days=DIAS_RETROATIVOS)
    
    data_atual = data_inicial

    # Loop dia a dia até chegar em hoje
    while data_atual <= data_final:
        processar_dia(session, banco_total, data_atual)
        
        # Salva a cada dia processado (segurança contra falhas)
        salvar_estado(banco_total)
        
        data_atual += timedelta(days=1)

    print("\n\n🏁 Varredura completa! Script finalizado para salvamento.")

if __name__ == "__main__":
    main()
