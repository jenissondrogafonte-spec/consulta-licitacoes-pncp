import json
import os
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ARQ_DADOS = 'dados.json'
# Reduzido para evitar bloqueios do firewall do Governo por excesso de velocidade
MAX_WORKERS = 3 

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def criar_sessao():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    # NOVO: backoff_factor aumentado e código 429 adicionado (Força o robô a esperar e tentar de novo se for bloqueado)
    retry = Retry(total=7, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount('https://', adapter)
    return session

def buscar_info_licitacao(session, lic_id):
    cnpj_org = lic_id[:14]
    seq = int(lic_id[14:-4])
    ano = lic_id[-4:]
    
    url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}"
    
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 200:
            dados = r.json()
            uasg = dados.get('unidadeOrgao', {}).get('codigoUnidade', '')
            data_fim = dados.get('dataEncerramentoProposta', '')
            return (lic_id, uasg, data_fim)
    except Exception:
        pass
        
    time.sleep(0.5) # Pausa estratégica caso haja erro
    return (lic_id, None, None)

def main():
    print("🔍 A iniciar o script de reparo cirúrgico...")
    if not os.path.exists(ARQ_DADOS):
        print("❌ Ficheiro dados.json não encontrado.")
        return

    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        banco = json.load(f)

    licitacoes_pendentes = set()
    for item in banco:
        if 'UASG' not in item or 'DataFimPropostas' not in item or item.get('UASG') == '' or item.get('DataFimPropostas') == '':
            lic_id = item.get('Licitacao')
            if lic_id:
                licitacoes_pendentes.add(lic_id)

    total_pendentes = len(licitacoes_pendentes)
    if total_pendentes == 0:
        print("✅ O banco de dados já está 100% atualizado.")
        return

    print(f"📦 Foram encontrados {total_pendentes} processos com informações em falta. A transferir dados do PNCP...")
    session = criar_sessao()
    resultados_reparo = {}
    processados = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(buscar_info_licitacao, session, lic_id) for lic_id in licitacoes_pendentes]
        for fut in concurrent.futures.as_completed(futures):
            lic_id, uasg, data_fim = fut.result()
            
            if uasg is not None or data_fim is not None:
                resultados_reparo[lic_id] = {
                    "UASG": uasg if uasg else '', 
                    "DataFimPropostas": data_fim if data_fim else ''
                }
            processados += 1
            print(f"\r⏳ Progresso: {processados}/{total_pendentes}", end="", flush=True)

    print("\n\n🛠️ A aplicar as correções no banco de dados...")
    itens_corrigidos = 0
    for item in banco:
        lic_id = item.get('Licitacao')
        if lic_id in resultados_reparo:
            if 'UASG' not in item or item.get('UASG') == '':
                item['UASG'] = resultados_reparo[lic_id]['UASG']
            if 'DataFimPropostas' not in item or item.get('DataFimPropostas') == '':
                item['DataFimPropostas'] = resultados_reparo[lic_id]['DataFimPropostas']
            itens_corrigidos += 1

    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(banco, f, indent=4, ensure_ascii=False)

    print(f"✅ Reparo concluído com sucesso! {itens_corrigidos} itens de edital foram atualizados.")

if __name__ == "__main__":
    main()
