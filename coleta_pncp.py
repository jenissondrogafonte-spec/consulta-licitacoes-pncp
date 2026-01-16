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

# --- 🎯 MODO SNIPER ---
CNPJ_ALVO = "" 

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if not env_inicio:
    hoje = datetime.now()
    d_ini = (hoje - timedelta(days=3)).strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')
else:
    d_ini, d_fim = env_inicio, env_fim

dict_novos = {}
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ TOTAL 2026 (ATUALIZAÇÕES GLOBAIS): {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Processando {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while True:
        # URL CORRIGIDA: Agora usamos o endpoint de atualização global
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/atualizacao"
        
        params = {
            "dataAtualizacaoInicial": DATA_STR,
            "dataAtualizacaoFinal": DATA_STR,
            "pagina": pagina, 
            "tamanhoPagina": 50
        }
        
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
                
            dados_json = resp.json()
            total_paginas = dados_json.get('totalPaginas', 1)
            lics = dados_json.get('data', [])
            
            if not lics: break
            print(".", end="", flush=True)

            for lic in lics:
                # FILTRO DE MODALIDADE: Filtramos aqui para garantir compatibilidade com a API
                # 6 = Pregão
                if str(lic.get('modalidadeId')) != '6':
                    continue

                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                
                item_id = lic.get('id')
                link_pncp = f"https://pncp.gov.br/app/editais/{item_id}"

                try:
                    time.sleep(0.05)
                    r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                    if r_it.status_code == 200:
                        itens_api = r_it.json()
                        resumo = {"Homologados": 0, "Fracassados": 0, "Desertos": 0, "Abertos": 0}
                        forn_local = {}
                        tem_algo = False 

                        for it in itens_api:
                            sit_item = (it.get('situacaoItemNome') or "").upper()
                            is_fracassado = "FRACASSADO" in sit_item or "CANCELADO" in sit_item
                            is_deserto = "DESERTO" in sit_item
                            
                            if it.get('temResultado'):
                                tem_algo = True
                                resumo["Homologados"] += 1
                                r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                if r_v.status_code == 200:
                                    vends = r_v.json()
                                    if isinstance(vends, dict): vends = [vends]
                                    for v in vends:
                                        cv = v.get('niFornecedor') or "SEM-CNPJ"
                                        if CNPJ_ALVO and CNPJ_ALVO not in cv: continue
                                        chave = f"{id_lic}-{cv}"
                                        if chave not in forn_local:
                                            forn_local[chave] = criar_estrutura(lic, uasg, seq, ano, cv, v.get('nomeRazaoSocialFornecedor'), id_lic, DATA_STR, link_pncp)
                                        forn_local[chave]["Itens"].append({
                                            "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                            "Qtd": v.get('quantidadeHomologada'), "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                            "Total": float(v.get('valorTotalHomologado') or 0), "Status": "Venceu"
                                        })
                            elif is_fracassado or is_deserto:
                                tem_algo = True
                                if is_fracassado: resumo["Fracassados"] += 1
                                else: resumo["Desertos"] += 1
                                chave_fail = f"{id_lic}-SEM_RESULTADO"
                                if chave_fail not in forn_local:
                                    forn_local[chave_fail] = criar_estrutura(lic, uasg, seq, ano, "---", "SEM VENCEDOR", id_lic, DATA_STR, link_pncp)
                                forn_local[chave_fail]["Itens"].append({
                                    "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                    "Qtd": it.get('quantidade') or 0, "Unitario": float(it.get('valorUnitarioEstimado') or 0),
                                    "Total": float(it.get('valorTotalEstimado') or 0), "Status": "Fracassado" if is_fracassado else "Deserto"
                                })
                            else:
                                tem_algo = True
                                resumo["Abertos"] += 1
                                chave_open = f"{id_lic}-ABERTO"
                                if chave_open not in forn_local:
                                    forn_local[chave_open] = criar_estrutura(lic, uasg, seq, ano, "---", "EM DISPUTA (ABERTO)", id_lic, DATA_STR, link_pncp)
                                forn_local[chave_open]["Itens"].append({
                                    "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                    "Qtd": it.get('quantidade') or 0, "Unitario": float(it.get('valorUnitarioEstimado') or 0),
                                    "Total": float(it.get('valorTotalEstimado') or 0), "Status": "Em Aberto"
                                })

                        if tem_algo:
                            for c, dados in forn_local.items():
                                dados["Resumo"] = resumo
                                dict_novos[c] = dados
                except: pass
            if pagina >= total_paginas: break
            pagina += 1
        except: break
    data_atual += timedelta(days=1)

def criar_estrutura(lic, uasg, seq, ano, cnpj, razao, id_lic, data_str, link):
    return {
        "DataResult": lic.get('dataAtualizacao') or data_str,
        "DataAbertura": lic.get('dataAberturaLicitacao', ''), 
        "DataEncerramento": lic.get('dataEncerramentoProposta', ''), 
        "Link": link, "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
        "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
        "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
        "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
        "Fornecedor": razao, "CNPJ": cnpj, "Licitacao": id_lic, "Itens": []
    }

historico = []
if os.path.exists(ARQ_DADOS):
    try:
        with open(ARQ_DADOS, 'r', encoding='utf-8') as f: historico = json.load(f)
    except: pass

banco = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
banco.update(dict_novos)
with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)
print(f"\n📊 Novos: {len(dict_novos)} | Total: {len(banco)}")
