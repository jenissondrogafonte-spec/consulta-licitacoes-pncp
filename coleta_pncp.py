import requests
import json
from datetime import datetime, timedelta
import os
import time

# --- CONFIGURAÇÃO ---
HEADERS = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
ARQ_DADOS = 'dados.json'

# --- 🎯 MODO SNIPER: COLOQUE O CNPJ AQUI PARA TESTAR UMA EMPRESA ---
CNPJ_ALVO = "08778201000126"  # Deixe "" vazio para buscar tudo
# ------------------------------------------------------------------

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if not env_inicio:
    # Se for busca por CNPJ específico, pegamos um intervalo MAIOR (o ano todo)
    # pois é rápido e queremos ver tudo o que ela ganhou.
    if CNPJ_ALVO:
        d_ini = "20250101"
        d_fim = datetime.now().strftime('%Y%m%d')
    else:
        hoje = datetime.now()
        d_ini = (hoje - timedelta(days=3)).strftime('%Y%m%d')
        d_fim = hoje.strftime('%Y%m%d')
else:
    d_ini, d_fim = env_inicio, env_fim

dict_novos = {}
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

modo_txt = f"CNPJ {CNPJ_ALVO}" if CNPJ_ALVO else "BRASIL TODO"
print(f"--- ROBÔ MODO {modo_txt}: {d_ini} até {d_fim} ---")

# Se for busca por CNPJ, não precisamos iterar dia a dia, a API aguenta períodos longos
# Mas manteremos a lógica para ser compatível
while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Verificando {DATA_STR}:", end=" ")
    
    pagina = 1
    while pagina <= 50: # Paginação menor pois CNPJ específico tem menos volume
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        
        params = {
            "dataInicial": DATA_STR, 
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", 
            "pagina": pagina, 
            "tamanhoPagina": 50
        }
        
        # O PULO DO GATO: Filtra direto na fonte
        if CNPJ_ALVO:
            params["niFornecedor"] = CNPJ_ALVO

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            lics = resp.json().get('data', [])
            if not lics: break
            
            print(".", end="", flush=True)

            for lic in lics:
                # Se buscamos CNPJ específico, aceitamos qualquer status para ver onde ele está
                situacoes_aceitas = ['4', '6', '10'] if not CNPJ_ALVO else ['1', '2', '3', '4', '6', '8', '10']
                
                if str(lic.get('situacaoCompraId')) in situacoes_aceitas:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    try:
                        time.sleep(0.1)
                        # Busca Itens
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                        if r_it.status_code == 200:
                            itens_api = r_it.json()
                            resumo = {"Homologados": 0, "Fracassados": 0, "Desertos": 0}
                            forn_local = {}

                            for it in itens_api:
                                sit = (it.get('situacaoItemNome') or "").upper()
                                if "FRACASSADO" in sit: resumo["Fracassados"] += 1
                                elif "DESERTO" in sit: resumo["Desertos"] += 1
                                
                                # Se tem CNPJ Alvo, queremos ver se ELE ganhou este item
                                if it.get('temResultado'):
                                    resumo["Homologados"] += 1
                                    r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                    if r_v.status_code == 200:
                                        vends = r_v.json()
                                        if isinstance(vends, dict): vends = [vends]
                                        for v in vends:
                                            cv = v.get('niFornecedor') or "SEM-CNPJ"
                                            
                                            # SE FOR MODO SNIPER, SÓ SALVA SE FOR O CNPJ ALVO
                                            if CNPJ_ALVO and CNPJ_ALVO not in cv:
                                                continue

                                            chave = f"{id_lic}-{cv}"
                                            if chave not in forn_local:
                                                forn_local[chave] = {
                                                    "DataResult": lic.get('dataAtualizacao') or DATA_STR,
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

print(f"\n\n📈 RESULTADO: {len(dict_novos)} novos contratos encontrados para o alvo.")

# Se for busca específica, podemos optar por LIMPAR o histórico antigo ou manter
# Aqui vou manter a lógica de adicionar
banco = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
banco.update(dict_novos)

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)

print(f"✅ FINALIZADO. Arquivo atualizado.")
