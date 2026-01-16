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
CNPJ_ALVO = "" 

# --- DATAS ---
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if not env_inicio:
    hoje = datetime.now()
    if CNPJ_ALVO:
        d_ini = "20250101"
        d_fim = hoje.strftime('%Y%m%d')
    else:
        d_ini = (hoje - timedelta(days=3)).strftime('%Y%m%d')
        d_fim = hoje.strftime('%Y%m%d')
else:
    d_ini, d_fim = env_inicio, env_fim

dict_novos = {}
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ COM DATAS DE ABERTURA ({d_ini} até {d_fim}) ---")
if CNPJ_ALVO: print(f"🎯 MODO SNIPER: {CNPJ_ALVO}")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Verificando {DATA_STR}:", end=" ")
    
    pagina = 1
    max_paginas = 50 if CNPJ_ALVO else 200 
    
    while pagina <= max_paginas:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6",
            "pagina": pagina, "tamanhoPagina": 50
        }
        if CNPJ_ALVO: params["niFornecedor"] = CNPJ_ALVO

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            lics = resp.json().get('data', [])
            if not lics: break
            
            print(".", end="", flush=True)

            for lic in lics:
                status_validos = ['1','2','3','4','6','8','10'] if CNPJ_ALVO else ['4','6','10']
                
                if str(lic.get('situacaoCompraId')) in status_validos:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    # --- NOVA CAPTURA DE DATAS ---
                    dt_abertura = lic.get('dataAberturaLicitacao', '')
                    dt_encerra = lic.get('dataEncerramentoProposta', '')
                    # -----------------------------

                    try:
                        time.sleep(0.1)
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                        if r_it.status_code == 200:
                            itens_api = r_it.json()
                            resumo = {"Homologados": 0, "Fracassados": 0, "Desertos": 0}
                            forn_local = {}

                            for it in itens_api:
                                sit = (it.get('situacaoItemNome') or "").upper()
                                if "FRACASSADO" in sit: resumo["Fracassados"] += 1
                                elif "DESERTO" in sit: resumo["Desertos"] += 1
                                
                                if it.get('temResultado'):
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
                                                forn_local[chave] = {
                                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
                                                    # NOVOS CAMPOS SALVOS AQUI
                                                    "DataAbertura": dt_abertura,
                                                    "DataEncerramento": dt_encerra,
                                                    # ------------------------
                                                    "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
                                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                                    "Fornecedor": v.get('nomeRazaoSocialFornecedor'),
                                                    "CNPJ": cv, "Licitacao": id_lic, "Itens": []
                                                }
                                            forn_local[chave]["Itens"].append({
                                                "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                                "Qtd": v.get('quantidadeHomologada'), 
                                                "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                                "Total": float(v.get('valorTotalHomologado') or 0), 
                                                "Status": "Venceu"
                                            })
                            
                            for c, dados in forn_local.items():
                                dados["Resumo"] = resumo
                                dict_novos[c] = dados
                    except: pass
            pagina += 1
        except: break
    data_atual += timedelta(days=1)

# --- SALVAMENTO ---
historico = []
if os.path.exists(ARQ_DADOS):
    try:
        with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
            historico = json.load(f)
    except: pass

print(f"\n\n📊 RESUMO:")
print(f"   - Registros anteriores: {len(historico)}")
print(f"   - Novos registros: {len(dict_novos)}")

banco = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
banco.update(dict_novos)

lista_final = list(banco.values())
with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(lista_final, f, indent=4, ensure_ascii=False)

print(f"✅ FINALIZADO! Arquivo atualizado.")
