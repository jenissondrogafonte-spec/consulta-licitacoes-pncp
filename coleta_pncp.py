import requests
import json
from datetime import datetime, timedelta
import os
import time

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126" 
DATA_LIMITE_FINAL = datetime(2025, 12, 31)
DIAS_POR_CICLO = 10

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {f"{i['Licitacao']}-{i['CNPJ']}": i for i in dados}
        except: pass
    return {}

def salvar_banco(banco):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)

def ler_checkpoint():
    if os.path.exists(ARQ_CHECKPOINT):
        with open(ARQ_CHECKPOINT, 'r') as f:
            return datetime.strptime(f.read().strip(), '%Y%m%d')
    return datetime(2025, 1, 1)

def salvar_checkpoint(data):
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data.strftime('%Y%m%d'))

def criar_estrutura(lic, uasg, seq, ano, cnpj, razao, id_lic, data_str, link):
    return {
        "DataResult": lic.get('dataAtualizacao') or data_str,
        "Link": link, "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
        "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
        "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
        "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
        "Fornecedor": razao, "CNPJ": cnpj, "Licitacao": id_lic, "Itens": []
    }

# --- PROCESSAMENTO ---
data_inicio = ler_checkpoint()
if data_inicio > DATA_LIMITE_FINAL:
    print("🎯 Ano 2025 concluído!")
    exit(0)

data_fim = data_inicio + timedelta(days=DIAS_POR_CICLO - 1)
if data_fim > DATA_LIMITE_FINAL: data_fim = DATA_LIMITE_FINAL

print(f"--- 🎯 SNIPER RESILIENTE: {data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')} ---")

banco_total = carregar_banco()
data_atual = data_inicio

while data_atual <= data_fim:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 {data_atual.strftime('%d/%m/%Y')}:", end=" ")
    
    pagina = 1
    achou_no_dia = 0
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {"dataInicial": DATA_STR, "dataFinal": DATA_STR, "codigoModalidadeContratacao": "6", "pagina": pagina, "tamanhoPagina": 50, "niFornecedor": CNPJ_ALVO}

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            lics = resp.json().get('data', [])
            if not lics: break
            print(f"[{len(lics)} editais]", end="", flush=True)

            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano, seq = lic.get('anoCompra'), lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                link_pncp = f"https://pncp.gov.br/app/editais/{lic.get('id')}"

                try:
                    time.sleep(0.15)
                    r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                    if r_it.status_code == 200:
                        itens_api = r_it.json()
                        for it in itens_api:
                            if it.get('temResultado'):
                                time.sleep(0.1)
                                r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                if r_v.status_code == 200:
                                    vends = r_v.json()
                                    if isinstance(vends, dict): vends = [vends]
                                    for v in vends:
                                        cv = (v.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                                        if CNPJ_ALVO in cv:
                                            achou_no_dia += 1
                                            chave = f"{id_lic}-{CNPJ_ALVO}"
                                            if chave not in banco_total:
                                                banco_total[chave] = criar_estrutura(lic, uasg, seq, ano, CNPJ_ALVO, v.get('nomeRazaoSocialFornecedor'), id_lic, DATA_STR, link_pncp)
                                            
                                            # Evita duplicar item dentro da mesma licitação
                                            if not any(x['Item'] == it.get('numeroItem') for x in banco_total[chave]["Itens"]):
                                                banco_total[chave]["Itens"].append({
                                                    "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                                    "Qtd": v.get('quantidadeHomologada'), "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                                    "Total": float(v.get('valorTotalHomologado') or 0), "Status": "Venceu"
                                                })
                                            print("🎯", end="", flush=True)
                except: continue
            if pagina >= resp.json().get('totalPaginas', 1): break
            pagina += 1
        except: break
    
    # --- SALVAMENTO DIÁRIO (SEGURANÇA) ---
    salvar_banco(banco_total)
    salvar_checkpoint(data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)

print(f"\n\n✅ Ciclo finalizado com sucesso.")
