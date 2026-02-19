import requests
import json
from datetime import datetime, timedelta
import os
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import sys

# --- CONFIGURAÇÕES ---
CNPJ_ALVO = "08778201000126"
MAX_WORKERS = 15
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT_ATUALIZADOR = 'checkpoint_atualizador.txt'
DIAS_RETROATIVOS = 365
JANELA_DIAS = 15

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
}

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                conteudo = json.loads(f.read())
                if isinstance(conteudo, list):
                    return {f"{item['Licitacao']}-{item['Item']}": item for item in conteudo if 'Licitacao' in item}
        except Exception as e:
            print(f"Erro ao carregar banco: {e}")
    return {}

def salvar_banco(banco):
    lista_final = list(banco.values())
    lista_final.sort(key=lambda x: x.get('DataResult', x.get('DataPublicacao', '')), reverse=True)
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(lista_final, f, indent=4, ensure_ascii=False)

def obter_resultados_item(session, cnpj_org, ano, seq, num_item):
    """Busca o resultado minucioso de um item específico no PNCP"""
    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{num_item}/resultados"
    try:
        r = session.get(url_res, timeout=20)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def processar_licitacao(session, lic_id, lic_base_data, banco_total):
    """Varre todos os itens de uma licitação para garantir que nada do CNPJ Alvo escape"""
    cnpj_org = lic_base_data.get('Licitacao')[:14]
    ano = lic_base_data.get('Licitacao')[-4:]
    seq = lic_base_data.get('Licitacao')[14:-4]
    
    p_it = 1
    itens_completos = []
    
    # Paginação minuciosa de itens
    while True:
        url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{int(seq)}/itens?pagina={p_it}&tamanhoPagina=500"
        try:
            r_it = session.get(url_itens, timeout=20)
            if r_it.status_code == 200:
                lista = r_it.json()
                if not lista: break
                itens_completos.extend(lista)
                if len(lista) < 500: break
                p_it += 1
            else:
                break
        except:
            break

    for it in itens_completos:
        if not it.get('temResultado'): continue
        
        num_item = it.get('numeroItem')
        resultados = obter_resultados_item(session, cnpj_org, ano, int(seq), num_item)
        
        if resultados:
            if isinstance(resultados, dict): resultados = [resultados]
            for v in resultados:
                ni = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                chave = f"{lic_base_data['Licitacao']}-{num_item}"
                
                # Se o alvo venceu, atualizamos/inserimos
                if CNPJ_ALVO in ni:
                    novo_dado = {
                        "Item": num_item,
                        "Descricao": it.get('descricao', ''),
                        "Qtd": v.get('quantidadeHomologada'),
                        "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                        "Total": float(v.get('valorTotalHomologado') or 0),
                        "Status": "Venceu",
                        "DataPublicacao": lic_base_data.get('DataPublicacao'),
                        "DataResult": lic_base_data.get('DataResult'),
                        "Orgao": lic_base_data.get('Orgao'),
                        "UF": lic_base_data.get('UF'),
                        "Municipio": lic_base_data.get('Municipio'),
                        "Edital": lic_base_data.get('Edital'),
                        "Licitacao": lic_base_data.get('Licitacao'),
                        "Link": lic_base_data.get('Link')
                    }
                    banco_total[chave] = novo_dado
                # Se o item existia na base do alvo, mas ele perdeu (mudança de status)
                elif chave in banco_total and banco_total[chave]['Status'] == "Venceu":
                    banco_total[chave]['Status'] = "Perdeu/Anulado"

def main():
    hoje = datetime.now()
    limite_retroativo = hoje - timedelta(days=DIAS_RETROATIVOS)
    
    # Define a data de início da janela atual
    data_inicio_janela = limite_retroativo
    if os.path.exists(ARQ_CHECKPOINT_ATUALIZADOR):
        try:
            with open(ARQ_CHECKPOINT_ATUALIZADOR, 'r') as f:
                data_inicio_janela = datetime.strptime(f.read().strip(), '%Y%m%d')
        except: pass

    # Se a janela passou de hoje, reseta para o fim da fila (reinicia o ciclo de 365 dias)
    if data_inicio_janela >= hoje:
        print("🔄 Ciclo completo atingido. Reiniciando a varredura a partir de 365 dias atrás.")
        data_inicio_janela = limite_retroativo

    data_fim_janela = data_inicio_janela + timedelta(days=JANELA_DIAS)
    if data_fim_janela > hoje:
        data_fim_janela = hoje

    print(f"🛠️ ATUALIZADOR: Varrendo publicações de {data_inicio_janela.strftime('%d/%m/%Y')} a {data_fim_janela.strftime('%d/%m/%Y')}")

    banco_total = carregar_banco()
    session = criar_sessao()

    # Mapear licitações únicas que caem nesta janela de tempo
    licitacoes_na_janela = {}
    for chave, dados in banco_total.items():
        data_pub_str = dados.get('DataPublicacao')
        if data_pub_str:
            try:
                data_pub = datetime.strptime(data_pub_str, '%Y%m%d')
                if data_inicio_janela <= data_pub <= data_fim_janela:
                    lic_id = dados.get('Licitacao')
                    if lic_id not in licitacoes_na_janela:
                        licitacoes_na_janela[lic_id] = dados
            except: continue

    print(f"📦 Encontradas {len(licitacoes_na_janela)} licitações únicas nesta janela para escrutínio.")

    # Processamento paralelo das licitações
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(processar_licitacao, session, lic_id, dados, banco_total) 
                   for lic_id, dados in licitacoes_na_janela.items()]
        for _ in concurrent.futures.as_completed(futures):
            print(".", end="", flush=True)

    print("\n💾 Salvando banco de dados...")
    salvar_banco(banco_total)

    # Atualiza o checkpoint
    nova_data_checkpoint = data_fim_janela + timedelta(days=1)
    with open(ARQ_CHECKPOINT_ATUALIZADOR, 'w') as f:
        f.write(nova_data_checkpoint.strftime('%Y%m%d'))

    # Sinaliza para o GitHub Actions se o ciclo acabou ou se deve continuar
    if nova_data_checkpoint >= hoje:
        print("✅ Ciclo atualizado 100%. Aguardando próximo acionamento agendado.")
        sys.exit(0) # Sucesso, não precisa de re-execução imediata
    else:
        print(f"⏳ Janela concluída. Próxima janela iniciará em {nova_data_checkpoint.strftime('%d/%m/%Y')}.")
        sys.exit(2) # Código 2 indica que há mais dados e o GH Actions deve acionar a próxima tarefa imediatamente

if __name__ == "__main__":
    main()
