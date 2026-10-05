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
import sys

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"   # DROGAFONTE
MAX_WORKERS = 20                
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
DIAS_RETROATIVOS = 365
TEMPO_LIMITE_SEGURO = 19800  # 5h 30min para salvar antes do timeout
JANELA_DIAS = 1  # Define quantos dias processar por ciclo de Action

DATA_INICIO_FORCADA = os.environ.get("DATA_INICIO_FORCADA", "") 
MODALIDADES_BUSCA = [6] 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

INICIO_EXECUCAO = time.time()

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

def remover_duplicidades(banco):
    banco_unico = {}
    rastreio_similaridade = {} 

    for chave_original, dados in banco.items():
        # Extrai CNPJ base (14 dígitos), data final YYYY-MM-DD, item e valor
        cnpj_base = dados.get('Licitacao', '')[:14]
        data_fim = str(dados.get('DataFimPropostas', ''))[:10]
        item_num = dados.get('Item', '')
        valor_unit = dados.get('Unitario', 0)

        # Chave de similaridade para detectar duplicatas estruturais
        chave_sim = f"{cnpj_base}-{item_num}-{data_fim}-{valor_unit}"

        if chave_sim in rastreio_similaridade:
            chave_antiga = rastreio_similaridade[chave_sim]
            item_existente = banco_unico[chave_antiga]

            # Fica com a homologação/atualização mais recente
            data_nova = dados.get('DataResult', '')
            data_velha = item_existente.get('DataResult', '')

            if data_nova > data_velha:
                del banco_unico[chave_antiga]
                banco_unico[chave_original] = dados
                rastreio_similaridade[chave_sim] = chave_original
        else:
            banco_unico[chave_original] = dados
            rastreio_similaridade[chave_sim] = chave_original

    return banco_unico

def salvar_estado(banco, proximo_dia):
    # Aplica a limpeza de duplicidades antes de salvar
    banco_limpo = remover_duplicidades(banco)
    
    lista_final = list(banco_limpo.values())
    lista_final.sort(key=lambda x: x.get('DataResult', ''), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)
    
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(proximo_dia.strftime('%Y%m%d'))
    print(f"\n 💾 [Salvo! Checkpoint atualizado para: {proximo_dia.strftime('%d/%m/%Y')}]", flush=True)

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

def processar_item_individual(session, it, cnpj_org, ano, seq):
    num_item = it.get('numeroItem')
    if not num_item: return None

    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    
    try:
        r = session.get(url_res, timeout=20)
        if r.status_code == 200:
            json_resp = r.json()
            vends = json_resp.get('data') if isinstance(json_resp, dict) and 'data' in json_resp else json_resp
            if isinstance(vends, dict): vends = [vends]
            
            for v in (vends or []):
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                if CNPJ_ALVO in ni:
                    qtd = v.get('quantidadeHomologada') or v.get('quantidade') or 0
                    unitario = float(v.get('valorUnitarioHomologado') or v.get('valorUnitario') or 0)
                    total = float(v.get('valorTotalHomologado') or v.get('valorTotal') or 0)
                    
                    return {
                        "Item": num_item,
                        "Descricao": it.get('descricao', ''),
                        "Qtd": qtd,
                        "Unitario": unitario,
                        "Total": total,
                        "Status": "Venceu",
                        # Extrai a data verdadeira do resultado
                        "DataRealHomologacao": v.get('dataResultado') or v.get('dataAtualizacao') or v.get('dataHomologacao')
                    }
    except: pass
    return None

def processar_dia_completo(session, banco_total, data_atual):
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {data_atual.strftime('%d/%m/%Y')}:")

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

                qtd_processos_pagina = len(lics)
                qtd_itens_pagina = 0
                qtd_processos_capturados = 0
                qtd_itens_capturados = 0

                for lic in lics:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                    id_lic_unico = f"{cnpj_org}{str(seq).zfill(5)}{ano}"
                    
                    itens_lic = []
                    p_it = 1
                    while True:
                        r_it = session.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens?pagina={p_it}&tamanhoPagina=500", timeout=20)
                        
                        if r_it.status_code == 200:
                            json_resp = r_it.json()
                            lista = json_resp.get('data') if isinstance(json_resp, dict) and 'data' in json_resp else json_resp
                            if not lista: break
                            itens_lic.extend(lista)
                            if len(lista) < 500: break
                            p_it += 1
                        else: break
                    
                    qtd_itens_pagina += len(itens_lic)
                    if not itens_lic: continue

                    itens_ganhos_neste_processo = 0

                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [executor.submit(processar_item_individual, session, it, cnpj_org, ano, seq) for it in itens_lic]
                        for fut in concurrent.futures.as_completed(futures):
                            res = fut.result()
                            if res:
                                # Remove a chave temporária para não poluir o JSON final, e usa como prioridade
                                data_real = res.pop("DataRealHomologacao", None)
                                
                                chave = f"{id_lic_unico}-{res['Item']}"
                                banco_total[chave] = {
                                    "DataPublicacao": DATA_STR,
                                    "DataResult": data_real or lic.get('dataAtualizacao') or DATA_STR,
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
                                itens_ganhos_neste_processo += 1
                    
                    if itens_ganhos_neste_processo > 0:
                        qtd_processos_capturados += 1
                        qtd_itens_capturados += itens_ganhos_neste_processo

                print(f"  Página {pagina} - {qtd_processos_pagina} processos - {qtd_itens_pagina} itens - {qtd_processos_capturados} processos e {qtd_itens_capturados} itens capturados.")

                if pagina >= dados.get('totalPaginas', 1): break
                pagina += 1
            except: break

def main():
    session = criar_sessao()
    banco_total = carregar_banco()
    hoje = datetime.now()
    
    if DATA_INICIO_FORCADA.strip():
        try:
            data_atual = datetime.strptime(DATA_INICIO_FORCADA.strip(), "%Y-%m-%d")
            print(f"--- ⚠️ MODO MANUAL: Iniciando a partir de {data_atual.strftime('%d/%m/%Y')} ---")
        except ValueError:
            print("❌ Erro no formato da data. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        data_atual = hoje - timedelta(days=DIAS_RETROATIVOS)
        if os.path.exists(ARQ_CHECKPOINT):
            try:
                with open(ARQ_CHECKPOINT, 'r') as f:
                    data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')
            except: pass

    if data_atual.date() >= hoje.date():
        print(f"🏁 Fila 100% concluída até o dia de hoje ({data_atual.strftime('%d/%m/%Y')}).")
        sys.exit(0)

    print(f"--- 🚀 INICIANDO COLETA (Janela de {JANELA_DIAS} dia(s) a partir de {data_atual.strftime('%d/%m/%Y')}) ---")

    dias_processados = 0
    while dias_processados < JANELA_DIAS and data_atual.date() <= hoje.date():
        processar_dia_completo(session, banco_total, data_atual)
        data_atual += timedelta(days=1)
        dias_processados += 1
        
        if (time.time() - INICIO_EXECUCAO) > TEMPO_LIMITE_SEGURO:
            print(f"\n⚠️ TEMPO LIMITE. Interrompendo a janela de coleta.")
            break

    salvar_estado(banco_total, data_atual)

    if data_atual.date() < hoje.date():
        print(f"\n⏳ Lote processado. Faltam mais dias até hoje. Acionando próxima rotina...")
        sys.exit(2)
    else:
        print("\n✅ Coleta atualizada com sucesso até o dia atual.")
        sys.exit(0)

if __name__ == "__main__":
    main()
