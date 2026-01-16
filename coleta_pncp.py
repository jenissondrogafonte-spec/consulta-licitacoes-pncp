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

# --- 🎯 MODO SNIPER (ATIVADO PARA DROGAFONTE) ---
# Agora o robô só vai buscar o que interessa a esta empresa
CNPJ_ALVO = "08778201000126" 

# --- CONFIGURAÇÃO DE DATAS ---
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

print(f"--- 🎯 MODO SNIPER ATIVADO: {CNPJ_ALVO} ---")
print(f"--- PERÍODO: {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while True:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina, 
            "tamanhoPagina": 50
        }
        
        # O filtro abaixo é o que faz a mágica da velocidade
        if CNPJ_ALVO: 
            params["niFornecedor"] = CNPJ_ALVO

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
                
            dados_json = resp.json()
            total_paginas = dados_json.get('totalPaginas', 1)
            lics = dados_json.get('data', [])
            
            if not lics: break
            print(f"(Lendo {len(lics)} editais alvo)", end="", flush=True)

            for lic in lics:
                cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '')).strip()
                id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                id_unico = lic.get('id')
                link_pncp = f"https://pncp.gov.br/app/editais/{id_unico}" if id_unico else f"https://pncp.gov.br/app/editais/{cnpj_org}/{ano}/{seq}"

                try:
                    # No Modo Sniper, o sleep pode ser menor pois faremos poucas chamadas
                    time.sleep(0.05) 
                    r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                    
                    if r_it.status_code == 200:
                        itens_api = r_it.json()
                        resumo = {"Homologados": 0, "Fracassados": 0, "Desertos": 0, "Abertos": 0}
                        forn_local = {}
                        movimentou = False 

                        for it in itens_api:
                            sit_item = (it.get('situacaoItemNome') or "").upper()
                            is_fracassado = "FRACASSADO" in sit_item or "CANCELADO" in sit_item
                            is_deserto = "DESERTO" in sit_item
                            
                            if it.get('temResultado'):
                                r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                if r_v.status_code == 200:
                                    vends = r_v.json()
                                    if isinstance(vends, dict): vends = [vends]
                                    for v in vends:
                                        cv = v.get('niFornecedor') or "SEM-CNPJ"
                                        
                                        # No modo sniper, pulamos fornecedores que não sejam o alvo
                                        if CNPJ_ALVO and CNPJ_ALVO not in cv: continue

                                        movimentou = True
                                        resumo["Homologados"] += 1
                                        chave = f"{id_lic}-{cv}"
                                        if chave not in forn_local:
                                            forn_local[chave] = criar_estrutura(lic, uasg, seq, ano, cv, v.get('nomeRazaoSocialFornecedor'), id_lic, DATA_STR, link_pncp)
                                        
                                        forn_local[chave]["Itens"].append({
                                            "Item": it.get('numeroItem'), "Desc": it.get('descricao'),
                                            "Qtd": v.get('quantidadeHomologada'), 
                                            "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                            "Total": float(v.get('valorTotalHomologado') or 0), 
                                            "Status": "Venceu"
                                        })
                            
                            # Se for Sniper, geralmente não precisamos salvar Fracassados/Abertos de outros
                            # Mas mantemos aqui se você quiser ver o histórico completo dos editais onde ela participou
                            elif is_fracassado or is_deserto:
                                # (Opcional: você pode comentar este bloco se quiser SÓ os ganhos)
                                pass 

                        if movimentou:
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

# --- SALVAMENTO ---
historico = []
if os.path.exists(ARQ_DADOS):
    try:
        with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
            historico = json.load(f)
    except: pass

banco = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
banco.update(dict_novos)

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)

print(f"\n\n📊 MODO SNIPER FINALIZADO:")
print(f"   - Encontrados para Drogafonte: {len(dict_novos)}")
print(f"✅ Base atualizada.")
