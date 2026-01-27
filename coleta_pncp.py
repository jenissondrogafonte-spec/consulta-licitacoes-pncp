import requests
import json
from datetime import datetime, timedelta
import os
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
MAX_WORKERS = 20               # Processamento paralelo
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DIAS_RETROATIVOS = 365

# LIMITE DE SEGURANÇA (5 Horas e 30 Minutos)
# O GitHub derruba com 6h. Paramos antes para garantir o salvamento.
TEMPO_LIMITE_SEGURO = 19800 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

INICIO_EXECUCAO = time.time()

# -------------------------------------------------
# 1. FUNÇÕES DE BANCO DE DADOS E ESTADO
# -------------------------------------------------
def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                return {f"{i['Licitacao']}-{i['Item']}": i for i in json.loads(f.read())}
        except: pass
    return {}

def salvar_estado(banco, proximo_dia_para_processar):
    """
    Salva o progresso.
    IMPORTANTE: 'proximo_dia_para_processar' é a data de onde o robô deve COMEÇAR na próxima vez.
    """
    # 1. Salva os dados das licitações
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('DataResult', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    # 2. Atualiza o checkpoint
    # Se o robô parar agora, ele sabe que deve começar deste dia na próxima vez
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia_para_processar.strftime('%Y%m%d'))
        
    print(f" 💾 [Salvo! Próximo dia na fila: {proximo_dia_para_processar.strftime('%d/%m/%Y')}]", end="", flush=True)

def ler_checkpoint():
    """Define o ponto de partida."""
    hoje = datetime.now()
    inicio_padrao = hoje - timedelta(days=DIAS_RETROATIVOS)

    if os.path.exists(ARQ_CHECKPOINT):
        try:
            with open(ARQ_CHECKPOINT, 'r') as f:
                data_lida = datetime.strptime(f.read().strip(), '%Y%m%d')
            
            # LÓGICA DE RESET QUINZENAL (Dias 1 e 16)
            if data_lida.date() >= hoje.date():
                if hoje.day in [1, 16]: 
                    print(f"🔄 Reset Quinzenal (Dia {hoje.day}): Iniciando nova varredura de 365 dias.")
                    return inicio_padrao
                else:
                    return data_lida # Já terminou, mantém no futuro
            
            if data_lida < inicio_padrao: return inicio_padrao
            return data_lida
        except: pass
    return inicio_padrao

def tempo_acabando():
    """Verifica se já passamos de 5h30m de execução."""
    return (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO

# -------------------------------------------------
# 2. WORKER INDIVIDUAL (Processa 1 Item)
# -------------------------------------------------
def processar_item_individual(session, it, cnpj_org, ano, seq):
    if not it.get('temResultado'): return None
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
                        "Descricao": it.get('descricao', ''),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu"
                    }
    except: pass
    return None

# -------------------------------------------------
# 3. PROCESSAR UM DIA INTEIRO
# -------------------------------------------------
def processar_dia_completo(session, banco_total, data_atual):
    """
    Lógica blindada: Entrou aqui, vai até o fim do dia.
    Não verifica tempo aqui dentro para não parar licitação pela metade.
    """
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Iniciando dia {data_atual.strftime('%d/%m/%Y')}...", end=" ", flush=True)
    
    pagina_edital = 1
    encontrou_algo = False

    while True:
        url_base = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": DATA_STR, "dataFinal": DATA_STR, "codigoModalidadeContratacao": "6", "pagina": pagina_edital, "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO}

        try:
            resp = session.get(url_base, params=params, timeout=30)
            if resp.status_code != 200: break
            lics = resp.json().get('data', [])
            if not lics: break

            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                
                # Paginação de itens
                todos_itens = []
                p_it = 1
                while True:
                    r_it = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina={p_it}&tamanhoPagina=1000", timeout=20)
                    if r_it.status_code == 200:
                        l = r_it.json()
                        if not l: break
                        todos_itens.extend(l)
                        if len(l) < 1000: break
                        p_it += 1
                    else: break
                
                if not todos_itens: continue

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in todos_itens]
                    for future in concurrent.futures.as_completed(futures):
                        res = future.result()
                        if res:
                            chave = f"{id_lic}-{res['Item']}"
                            banco_total[chave] = {
                                "DataPublicacao": DATA_STR,
                                "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                "UASG": uasg,
                                "Edital": f"{lic.get('numeroCompra')}/{ano}",
                                "Licitacao": id_lic,
                                "Link": f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}",
                                **res
                            }
                            print("✅", end="", flush=True)
                            encontrou_algo = True

            if pagina_edital >= resp.json().get('totalPaginas', 1): break
            pagina_edital += 1
        except Exception as e:
            print(f"[Erro: {e}]", end="")
            break
    
    if not encontrou_algo:
        print("(Sem vitórias)", end="", flush=True)

# -------------------------------------------------
# 4. LOOP PRINCIPAL (CONTROLADO)
# -------------------------------------------------
def main():
    session = criar_sessao()
    banco_total = carregar_banco()
    
    data_atual = ler_checkpoint()
    data_final = datetime.now()
    
    print(f"--- 🚀 INICIANDO (Fila: {data_atual.strftime('%d/%m/%Y')} até {data_final.strftime('%d/%m/%Y')}) ---")
    
    if data_atual.date() > data_final.date():
        print("💤 Nada a fazer. Aguardando reset quinzenal (Dia 1 ou 16).")
        return

    while data_atual.date() <= data_final.date():
        
        # 1. Processa o dia inteiro (sem interrupção no meio)
        processar_dia_completo(session, banco_total, data_atual)
        
        # 2. Prepara o checkpoint para o DIA SEGUINTE
        data_proxima = data_atual + timedelta(days=1)
        
        # 3. Salva tudo (Dados + Checkpoint apontando para amanhã)
        salvar_estado(banco_total, data_proxima)
        
        # 4. Verifica o relógio APÓS salvar
        if tempo_acabando():
            print("\n\n⚠️ TEMPO LIMITE DE SEGURANÇA ATINGIDO (5h30m).")
            print(f"⏸️ Parando no dia {data_atual.strftime('%d/%m')}. O próximo ciclo continuará do dia {data_proxima.strftime('%d/%m')}.")
            break # Sai do loop, o script termina, GitHub salva o commit.
            
        # Se tem tempo, o loop continua e pega o 'data_proxima' (que agora é o data_atual atualizado na linha 164)
        data_atual = data_proxima

    print("\n\n🏁 Script finalizado.")

if __name__ == "__main__":
    main()
