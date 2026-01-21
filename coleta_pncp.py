import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURAÇÕES GERAIS ---
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
DIAS_POR_CICLO = 1             # Processa 1 dia por vez para segurança
MAX_WORKERS = 10               # Número de verificações simultâneas (Ideal para nuvem)
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime(2025, 12, 31)

# Desativa avisos de SSL (necessário para o site do governo)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def criar_sessao():
    """Cria uma sessão HTTP robusta com reconexão automática."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    # Configura retry para falhas de conexão (erros 500, 502, 504)
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                # Cria um índice único para evitar duplicatas
                return {f"{i['Licitacao']}-{i['Item']}": i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    # Converte o dicionário de volta para lista
    lista_final = list(banco.values())
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f" 💾 [Salvo: {len(lista_final)} registros]", end="", flush=True)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1) # Data padrão de início se não houver checkpoint

# --- FUNÇÃO WORKER (Processa 1 item) ---
def processar_item_individual(session, it, cnpj_org, ano, seq):
    """Verifica se a DROGAFONTE ganhou este item específico."""
    
    # Filtro Rápido: Se a API diz que não tem resultado, pulamos
    if not it.get('temResultado'):
        return None

    numero_item = it.get('numeroItem')
    # URL específica do resultado do item
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{numero_item}/resultados"
    
    try:
        r_v = session.get(url_res, timeout=20)
        if r_v.status_code == 200:
            vends = r_v.json()
            if isinstance(vends, dict): vends = [vends]
            
            # Varre os vencedores desse item
            for v in vends:
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                
                # SE FOR A DROGAFONTE
                if CNPJ_ALVO in ni:
                    return {
                        "Item": numero_item,
                        "Desc": it.get('descricao', 'Sem descrição'),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu"
                    }
    except:
        pass # Falhas de conexão pontuais são ignoradas para não parar o robô
    return None

# --- LOOP PRINCIPAL ---
def main():
    data_inicio = ler_checkpoint()
    
    if data_inicio > DATA_LIMITE_FINAL:
        print("🎯 Missão Cumprida! Todas as datas processadas.")
        return

    data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
    if data_fim > DATA_LIMITE_FINAL: data_fim = DATA_LIMITE_FINAL

    print(f"--- 🚀 INICIANDO VARREDURA: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")
    
    session = criar_sessao()
    banco_total = carregar_banco()
    data_atual = data_inicio

    while data_atual <= data_fim:
        DATA_STR = data_atual.strftime('%Y%m%d')
        print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
        
        # Paginação dos EDITAIS (Contratos)
        pagina_edital = 1
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
                "codigoModalidadeContratacao": "6", # Pregão
                "pagina": pagina_edital, 
                "tamanhoPagina": 50, 
                "niFornecedor": CNPJ_ALVO
            }

            try:
                resp = session.get(url, params=params, timeout=30)
                if resp.status_code != 200: break
                
                json_resp = resp.json()
                lics = json_resp.get('data', [])
                if not lics: break # Fim dos editais do dia

                print(f"[Pág {pagina_edital}: {len(lics)} editais]", end=" ", flush=True)

                for lic in lics:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                    
                    # Identificação única do Edital
                    chave_edital = f"{id_lic}"
                    
                    # --- NOVO: BUSCA TODOS OS ITENS (Paginação Infinita) ---
                    todos_itens_api = []
                    pag_item = 1
                    while True:
                        # Pede 1000 itens por vez para garantir que vem tudo
                        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina={pag_item}&tamanhoPagina=1000"
                        try:
                            r_it = session.get(url_itens, timeout=20)
                            if r_it.status_code == 200:
                                lote = r_it.json()
                                if not lote: break
                                todos_itens_api.extend(lote)
                                if len(lote) < 1000: break # Última página
                                pag_item += 1
                            else: break
                        except: break
                    
                    if not todos_itens_api: continue

                    # --- PROCESSAMENTO PARALELO DOS ITENS ---
                    itens_vencidos = []
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = []
                        for it in todos_itens_api:
                            futures.append(executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq))
                        
                        for future in concurrent.futures.as_completed(futures):
                            res = future.result()
                            if res:
                                itens_vencidos.append(res)
                                print("✅", end="", flush=True)

                    # --- SALVAR NO BANCO ---
                    if itens_vencidos:
                        for item_ganho in itens_vencidos:
                            # Chave única: ID_Licitação + Numero_Item
                            chave_unica = f"{id_lic}-{item_ganho['Item']}"
                            
                            if chave_unica not in banco_total:
                                banco_total[chave_unica] = {
                                    "DataPublicacao": DATA_STR,
                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                    "Edital": f"{str(seq).zfill(5)}/{ano}",
                                    "Licitacao": id_lic,
                                    "Item": item_ganho['Item'],
                                    "Descricao": item_ganho['Desc'],
                                    "Qtd": item_ganho['Qtd'],
                                    "Unitario": item_ganho['Unitario'],
                                    "Total": item_ganho['Total'],
                                    "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"
                                }

                # Fim do loop de editais na página atual
                if pagina_edital >= json_resp.get('totalPaginas', 1): break
                pagina_edital += 1
                
                # Salva parcialmente a cada página de editais processada
                salvar_estado(banco_total, data_atual)

            except Exception as e:
                print(f"Erro no dia {DATA_STR}: {e}")
                break
        
        # Avança para o próximo dia
        data_atual += timedelta(days=1)
        salvar_estado(banco_total, data_atual)

if __name__ == "__main__":
    main()
