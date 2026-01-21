import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Desativa avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"
DATA_LIMITE_FINAL = datetime(2025, 12, 31)
DIAS_POR_CICLO = 1 
MAX_WORKERS = 20  # Número de verificação simultâneas (Ajuste se sua internet engasgar)

# --- CONFIGURAÇÃO DA SESSÃO (OTIMIZAÇÃO) ---
def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    # Aumenta o pool de conexões para suportar as threads
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {f"{i['Licitacao']}-{i['CNPJ']}": i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    # Feedback visual simplificado para não poluir o console rápido
    # print(f" 💾", end="", flush=True)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return datetime.strptime(f.read().strip(), '%Y%m%d')
    return datetime(2025, 1, 1)

# --- FUNÇÃO WORKER (RODA EM PARALELO) ---
def processar_item_individual(session, it, cnpj_org, ano, seq):
    """Verifica se o CNPJ alvo ganhou este item específico."""
    if not it.get('temResultado'):
        return None

    numero_item = it.get('numeroItem')
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{numero_item}/resultados"
    
    try:
        r_v = session.get(url_res, timeout=10)
        if r_v.status_code == 200:
            vends = r_v.json()
            if isinstance(vends, dict): vends = [vends]
            
            for v in vends:
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                if CNPJ_ALVO in ni:
                    # Retorna os dados formatados se achou o vencedor
                    return {
                        "Item": numero_item,
                        "Desc": it.get('descricao'),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu"
                    }
    except:
        pass
    return None

# --- INÍCIO ---
data_inicio = ler_checkpoint()
if data_inicio > DATA_LIMITE_FINAL:
    print("🎯 Missão concluída!")
    exit(0)

data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
if data_fim > DATA_LIMITE_FINAL: data_fim = DATA_LIMITE_FINAL

print(f"--- 🚀 SNIPER TURBO V2 (MULTITHREADING ATIVO: {MAX_WORKERS} Threads) ---")
print(f"--- ALVO: {CNPJ_ALVO} | JANELA: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")

banco_total = carregar_banco()
data_atual = data_inicio
session = criar_sessao() # Sessão global

while data_atual <= data_fim:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    
    pagina = 1
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": "6", "pagina": pagina, 
            "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO
        }

        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code != 200: break
            
            json_resp = resp.json()
            lics = json_resp.get('data', [])
            if not lics: break
            print(f"[{len(lics)} editais]", end=" ", flush=True)

            for idx, lic in enumerate(lics):
                if idx % 5 == 0 and idx > 0: 
                    salvar_estado(banco_total, data_atual)
                    print(".", end="", flush=True)

                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                num_edital_real = lic.get('numeroCompra')
                link_custom = f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"
                chave = f"{id_lic}-{CNPJ_ALVO}"

                # Otimização: Se já tem itens salvos, pula
                if chave in banco_total and len(banco_total[chave]["Itens"]) > 0:
                    continue

                try:
                    # Busca lista de itens (1 Request)
                    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=3000"
                    r_it = session.get(url_itens, timeout=20)
                    
                    if r_it.status_code == 200:
                        itens_api = r_it.json()
                        
                        # --- INÍCIO DO PARALELISMO ---
                        novos_itens_encontrados = []
                        
                        # Cria um executor para processar itens em paralelo
                        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                            futures = []
                            for it in itens_api:
                                # Submete a tarefa para um worker
                                futures.append(executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq))
                            
                            # Coleta resultados conforme ficam prontos
                            for future in concurrent.futures.as_completed(futures):
                                resultado = future.result()
                                if resultado:
                                    novos_itens_encontrados.append(resultado)
                                    print("🎯", end="", flush=True)

                        # Se achou itens, salva no dicionário principal
                        if novos_itens_encontrados:
                            if chave not in banco_total:
                                banco_total[chave] = {
                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                    "DtInicioPropostas": lic.get('dataInicioRecebimentoPropostas'),
                                    "DtFimPropostas": lic.get('dataFimRecebimentoPropostas'),
                                    "IdPNCP": lic.get('idContratacaoPncp'),
                                    "NumEdital": f"{num_edital_real}/{ano}", 
                                    "Link": link_custom,
                                    "UASG": uasg, 
                                    "Edital": f"{str(seq).zfill(5)}/{ano}",
                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                    "Fornecedor": lic.get('fornecedor', {}).get('nomeRazaoSocial') or "N/A", # Fallback visual
                                    "CNPJ": CNPJ_ALVO, 
                                    "Licitacao": id_lic, 
                                    "Itens": []
                                }
                            
                            # Adiciona evitando duplicatas
                            ids_existentes = {x['Item'] for x in banco_total[chave]["Itens"]}
                            for novo in novos_itens_encontrados:
                                if novo['Item'] not in ids_existentes:
                                    banco_total[chave]["Itens"].append(novo)

                except Exception as e:
                    # print(f"Erro no edital: {e}") 
                    continue
            
            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e:
            print(f"Erro na paginação: {e}")
            break
    
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)

print(f"\n\n✅ Ciclo concluído. Total no banco: {len(banco_total)}")
