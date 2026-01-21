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
DIAS_POR_CICLO = 1             # Processa 1 dia por vez (ajuste para 30 se quiser buscar o passado)
MAX_WORKERS = 20               # Velocidade turbo (processos simultâneos)
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DATA_LIMITE_FINAL = datetime.now() # Coleta até o dia de hoje

# Desativa avisos de SSL (necessário para o site do governo)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# -------------------------------------------------
# MOTOR DE CONEXÃO E BANCO DE DADOS
# -------------------------------------------------
def criar_sessao():
    """Cria uma sessão HTTP robusta com reconexão automática."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def carregar_banco():
    """Carrega os dados existentes para ACUMULAR resultados em vez de sobrescrever."""
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if not conteudo: return {}
                dados = json.loads(conteudo)
                # Cria índice único [Licitação-Item] para evitar duplicatas ao mesclar
                return {f"{i['Licitacao']}-{i['Item']}": i for i in dados}
        except Exception as e:
            print(f"⚠️ Aviso: Erro ao ler banco existente ({e}). Iniciando novo.")
    return {}

def salvar_estado(banco, data_proxima):
    """Salva a lista completa (antigos + novos) e avança o checkpoint."""
    lista_final = list(banco.values())
    # Ordena por data (mais recentes primeiro) para facilitar a leitura no JSON
    lista_final.sort(key=lambda x: x.get('DataPublicacao', ''), reverse=True)
    
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f" 💾 [Total no Banco: {len(lista_final)} registros]", end="", flush=True)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                return datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass
    return datetime(2025, 1, 1)

# -------------------------------------------------
# WORKER: PROCESSAMENTO DE ITEM
# -------------------------------------------------
def processar_item_individual(session, it, cnpj_org, ano, seq):
    """Verifica se a DROGAFONTE ganhou este item específico."""
    if not it.get('temResultado'):
        return None

    numero_item = it.get('numeroItem')
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{numero_item}/resultados"
    
    try:
        r_v = session.get(url_res, timeout=20)
        if r_v.status_code == 200:
            vends = r_v.json()
            if isinstance(vends, dict): vends = [vends]
            
            for v in vends:
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
# LOOP PRINCIPAL
# -------------------------------------------------
def main():
    data_inicio = ler_checkpoint()
    
    if data_inicio.date() > DATA_LIMITE_FINAL.date():
        print("🎯 Checkpoint atualizado. Nada a processar hoje.")
        return

    data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
    if data_fim > DATA_LIMITE_FINAL: data_fim = DATA_LIMITE_FINAL

    print(f"--- 🚀 SNIPER ACUMULATIVO: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")
    
    session = criar_sessao()
    banco_total = carregar_banco() # Carrega os dados antigos aqui
    data_atual = data_inicio

    while data_atual <= data_fim:
        DATA_STR = data_atual.strftime('%Y%m%d')
        print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
        
        pagina_edital = 1
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": DATA_STR, "dataFinal": DATA_STR, 
                "codigoModalidadeContratacao": "6", "pagina": pagina_edital, 
                "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO
            }

            try:
                resp = session.get(url, params=params, timeout=30)
                if resp.status_code != 200: break
                
                json_resp = resp.json()
                lics = json_resp.get('data', [])
                if not lics: break

                for lic in lics:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                    
                    # 1. Busca Todos os Itens do Edital (Paginação Segura)
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

                    # 2. Processamento Paralelo de Resultados
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in todos_itens_api]
                        
                        for future in concurrent.futures.as_completed(futures):
                            res = future.result()
                            if res:
                                # Adiciona ou Atualiza no banco (A Chave evita duplicar se rodar o mesmo dia 2x)
                                chave_unica = f"{id_lic}-{res['Item']}"
                                banco_total[chave_unica] = {
                                    "DataPublicacao": DATA_STR,
                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                    "UASG": uasg,
                                    "Edital": f"{str(seq).zfill(5)}/{ano}",
                                    "Licitacao": id_lic,
                                    "IdPNCP": lic.get('idContratacaoPncp'),
                                    "DtInicioPropostas": lic.get('dataInicioRecebimentoPropostas'),
                                    "DtFimPropostas": lic.get('dataFimRecebimentoPropostas'),
                                    "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                                    **res # Acopla Item, Descricao, Qtd, Unitario, Total, Status
                                }
                                print("✅", end="", flush=True)

                if pagina_edital >= json_resp.get('totalPaginas', 1): break
                pagina_edital += 1
                salvar_estado(banco_total, data_atual)

            except Exception as e:
                print(f"Erro na página {pagina_edital}: {e}")
                break
        
        data_atual += timedelta(days=1)
        salvar_estado(banco_total, data_atual)

if __name__ == "__main__":
    main()
