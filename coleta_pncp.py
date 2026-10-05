import requests
import json
from datetime import datetime, timedelta
import os
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import re

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
MAX_WORKERS = 20                
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DIAS_RETROATIVOS = 365
TEMPO_LIMITE_SEGURO = 19800  # 5h 30min para salvar antes do timeout do GitHub

# Para forçar a varredura a partir de uma data específica, preencha abaixo (Ex: "2026-01-01").
# Para usar o ciclo contínuo automático do checkpoint, deixe vazio "".
DATA_INICIO_FORCADA = os.environ.get("DATA_INICIO_FORCADA", "")

# Configurado exclusivamente para Pregão Eletrônico (Código 6)
MODALIDADES_BUSCA = [6] 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

INICIO_EXECUCAO = time.time()

# -------------------------------------------------
# 1. FUNÇÕES DE SUPORTE
# -------------------------------------------------

def migrar_e_limpar_banco(dados_lista):
    novo_banco = {}
    for item in dados_lista:
        link = item.get('Link', '')
        match = re.search(r'editais/(\d+)/(\d+)/(\d+)', link)
        if match:
            cnpj_real, ano_real, seq_real = match.groups()
            novo_id_lic = f"{cnpj_real}{str(seq_real).zfill(5)}{ano_real}"
            item['Licitacao'] = novo_id_lic
            nova_chave = f"{novo_id_lic}-{item['Item']}"
            novo_banco[nova_chave] = item
    return novo_banco

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = json.loads(f.read())
                return migrar_e_limpar_banco(conteudo)
        except: pass
    return {}

def salvar_estado(banco, proximo_dia=None):
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('DataResult', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    if proximo_dia:
        with open(ARQ_CHECKPOINT, 'w') as f:
            f.write(proximo_dia.strftime('%Y%m%d'))
        print(f" 💾 [Salvo! Checkpoint atualizado para: {proximo_dia.strftime('%d/%m/%Y')}]", flush=True)
    else:
        print(" 💾 [Banco de dados salvo pelo modo manual]", flush=True)

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

# -------------------------------------------------
# 2. CAPTURA DE ITENS
# -------------------------------------------------

def processar_item_individual(session, it, cnpj_org, ano, seq):
    if not it.get('temResultado'): return None
    num_item = it.get('numeroItem')
    url_res = f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj_org}/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url_res, timeout=20)
        if r.status_code == 200:
            json_resp = r.json()
            vends = json_resp.get('data') if isinstance(json_resp, dict) and 'data' in json_resp else json_resp
            if isinstance(vends, dict): vends = [vends]
            
            for v in (vends or []):
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                if CNPJ_ALVO in ni:
                    return {
                        "Item": num_item,
                        "Descricao": it.get('descricao', ''),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu"
                    }
    except: pass
    return None

def processar_dia_completo(session, banco_total, data_atual):
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:", end=" ", flush=True)
    encontrou_no_dia = False

    for modalidade in MODALIDADES_BUSCA:
        pagina = 1
        
        while True:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {"dataInicial": DATA_STR, "dataFinal": DATA_STR, "codigoModalidadeContratacao": str(modalidade), "pagina": pagina, "tamanhoPagina": 50}

            try:
                resp = session.get(url, params=params, timeout=30)
                if resp.status_code != 200: break
                dados = resp.json(); lics = dados.get('data', [])
                if not lics: break

                for lic in lics:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                    id_lic_unico = f"{cnpj_org}{str(seq).zfill(5)}{ano}"
                    
                    itens_lic = []
                    p_it = 1
                    while True:
                        r_it = session.get(f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj_org}/{ano}/{seq}/itens?pagina={p_it}&tamanhoPagina=500", timeout=20)
                        if r_it.status_code == 200:
                            json_resp = r_it.json()
                            lista = json_resp.get('data') if isinstance(json_resp, dict) and 'data' in json_resp else json_resp
                            if not lista: break
                            itens_lic.extend(lista)
                            if len(lista) < 500: break
                            p_it += 1
                        else: break
                    
                    if not itens_lic: continue

                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in itens_lic]
                        for fut in concurrent.futures.as_completed(futures):
                            res = fut.result()
                            if res:
                                chave = f"{id_lic_unico}-{res['Item']}"
                                banco_total[chave] = {
                                    "DataPublicacao": DATA_STR,
                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                    "Edital": f"{lic.get('numeroCompra')}/{ano}",
                                    "Licitacao": id_lic_unico,
                                    "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                                    "UASG": lic.get('unidadeOrgao', {}).get('codigoUnidade', ''),
                                    "DataFimPropostas": lic.get('dataEncerramentoProposta', ''),
                                    **res
                                }
                                print(f"✅", end="", flush=True); encontrou_no_dia = True

                if pagina >= dados.get('totalPaginas', 1): break
                pagina += 1
            except: break
            
    if not encontrou_no_dia: print("(vazio)", end="", flush=True)

# -------------------------------------------------
# 3. CONTROLE DE EXECUÇÃO
# -------------------------------------------------

def main():
    session = criar_sessao()
    banco_total = carregar_banco()
    hoje = datetime.now()
    
    # Define a data inicial baseada na configuração manual ou no checkpoint
    modo_manual = False
    if DATA_INICIO_FORCADA.strip():
        try:
            data_atual = datetime.strptime(DATA_INICIO_FORCADA.strip(), "%Y-%m-%d")
            modo_manual = True
            print(f"--- ⚠️ MODO MANUAL ATIVADO: Ignorando checkpoint e iniciando em {data_atual.strftime('%d/%m/%Y')} ---")
        except ValueError:
            print("❌ Erro: O formato de DATA_INICIO_FORCADA deve ser YYYY-MM-DD. A abortar.")
            return
    else:
        data_atual = hoje - timedelta(days=DIAS_RETROATIVOS)
        if os.path.exists(ARQ_CHECKPOINT):
            try:
                with open(ARQ_CHECKPOINT, 'r') as f:
                    data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

        if data_atual.date() >= hoje.date():
            print(f"🏁 O robô já completou a fila até hoje ({data_atual.strftime('%d/%m/%Y')}).")
            return
        print(f"--- 🚀 INICIANDO COLETA CONTÍNUA (De: {data_atual.strftime('%d/%m/%Y')}) ---")

    while data_atual.date() <= hoje.date():
        processar_dia_completo(session, banco_total, data_atual)
        data_proxima = data_atual + timedelta(days=1)
        
        # Salva o progresso no final do dia. Se for modo manual, não atualiza o checkpoint para não atrapalhar o ciclo do Actions.
        if modo_manual:
            salvar_estado(banco_total, None)
        else:
            salvar_estado(banco_total, data_proxima)
        
        # Verifica tempo de execução para evitar corte brusco
        if (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO:
            print(f"\n\n⚠️ TEMPO LIMITE SEGURO ATINGIDO. A parar em {data_atual.strftime('%d/%m')}.")
            break
        
        data_atual = data_proxima

if __name__ == "__main__":
    main()
