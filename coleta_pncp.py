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
TEMPO_LIMITE_SEGURO = 19800  

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

INICIO_EXECUCAO = time.time()

# -------------------------------------------------
# 1. FUNÇÕES DE MANUTENÇÃO E BANCO
# -------------------------------------------------

def migrar_e_limpar_banco(dados_lista):
    """
    Corrige dados antigos onde licitações de órgãos diferentes colidiram.
    Usa o CNPJ contido no link para reconstruir a identidade única.
    """
    novo_banco = {}
    print("🔧 Verificando integridade dos dados existentes...")
    
    for item in dados_lista:
        link = item.get('Link', '')
        # Extrai CNPJ, Ano e Sequencial do link: .../editais/{cnpj}/{ano}/{seq}
        match = re.search(r'editais/(\d+)/(\d+)/(\d+)', link)
        if match:
            cnpj_real = match.group(1)
            ano_real = match.group(2)
            seq_real = match.group(3)
            
            # Reconstrói o ID único indestrutível
            novo_id_lic = f"{cnpj_real}{str(seq_real).zfill(5)}{ano_real}"
            item['Licitacao'] = novo_id_lic
            
            # Nova chave para o dicionário (ID + Item)
            nova_chave = f"{novo_id_lic}-{item['Item']}"
            novo_banco[nova_chave] = item
    
    if len(novo_banco) != len(dados_lista):
        print(f"✨ Migração concluída: {len(novo_banco)} itens únicos identificados.")
    return novo_banco

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = json.loads(f.read())
                # Aplica a migração para corrigir IDs antigos
                return migrar_e_limpar_banco(conteudo)
        except Exception as e:
            print(f"Erro ao carregar banco: {e}")
    return {}

def salvar_estado(banco, proximo_dia):
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('DataResult', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
    print(f" 💾 [Salvo! Próximo: {proximo_dia.strftime('%d/%m/%Y')}]", end="", flush=True)

# -------------------------------------------------
# 2. CORE DO PROCESSO
# -------------------------------------------------

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

def processar_item_individual(session, it, cnpj_org, ano, seq):
    if not it.get('temResultado'): return None
    num_item = it.get('numeroItem')
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url_res, timeout=20)
        if r.status_code == 200:
            vends = r.json()
            if isinstance(vends, dict): vends = [vends]
            for v in vends:
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
    print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}...", end=" ", flush=True)
    
    pagina = 1
    encontrou = False

    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, 
            "dataFinal": DATA_STR, 
            "codigoModalidadeContratacao": "6", 
            "pagina": pagina, 
            "tamanhoPagina": 50, 
            "niFornecedor": CNPJ_ALVO
        }

        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code != 200: break
            dados = resp.json()
            lics = dados.get('data', [])
            if not lics: break

            for lic in lics:
                # DADOS DO ÓRGÃO (A chave mestra)
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                
                # NOVO ID IDENTIFICADOR: CNPJ + SEQUENCIAL + ANO
                id_lic_unico = f"{cnpj_org}{str(seq).zfill(5)}{ano}"
                
                # Coleta de itens
                itens_lic = []
                p_it = 1
                while True:
                    r_it = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina={p_it}&tamanhoPagina=1000", timeout=20)
                    if r_it.status_code == 200:
                        lista = r_it.json()
                        if not lista: break
                        itens_lic.extend(lista)
                        if len(lista) < 1000: break
                        p_it += 1
                    else: break
                
                if not itens_lic: continue

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in itens_lic]
                    for fut in concurrent.futures.as_completed(futures):
                        res = fut.result()
                        if res:
                            # Chave do dicionário agora usa o ID ÚNICO baseado no CNPJ
                            chave = f"{id_lic_unico}-{res['Item']}"
                            banco_total[chave] = {
                                "DataPublicacao": DATA_STR,
                                "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                "UASG": uasg,
                                "Edital": f"{lic.get('numeroCompra')}/{ano}",
                                "Licitacao": id_lic_unico,
                                "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                                **res
                            }
                            print("✅", end="", flush=True)
                            encontrou = True

            if pagina >= dados.get('totalPaginas', 1): break
            pagina += 1
        except Exception as e:
            print(f"[Erro: {e}]", end="")
            break
    
    if not encontrou: print("(vazio)", end="", flush=True)

# -------------------------------------------------
# 3. LOOP PRINCIPAL
# -------------------------------------------------

def ler_checkpoint():
    hoje = datetime.now()
    padrao = hoje - timedelta(days=DIAS_RETROATIVOS)
    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                dt = datetime.strptime(f.read().strip(), '%Y%m%d')
                if dt.date() >= hoje.date() and hoje.day in [1, 16]: return padrao
                return dt
        except: pass
    return padrao

def main():
    session = criar_sessao()
    banco_total = carregar_banco()
    data_atual = ler_checkpoint()
    data_final = datetime.now()
    
    print(f"--- 🚀 INICIANDO COLETA (Fila: {data_atual.strftime('%d/%m/%Y')}) ---")
    
    while data_atual.date() <= data_final.date():
        processar_dia_completo(session, banco_total, data_atual)
        
        data_proxima = data_atual + timedelta(days=1)
        salvar_estado(banco_total, data_proxima)
        
        if (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO:
            print(f"\n\n⚠️ TEMPO LIMITE. Parando em {data_atual.strftime('%d/%m')}.")
            break
        
        data_atual = data_proxima

    print("\n\n🏁 Finalizado.")

if __name__ == "__main__":
    main()
