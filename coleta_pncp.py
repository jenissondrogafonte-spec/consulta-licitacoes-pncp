import requests
import json
from datetime import datetime, timedelta
import os
import time

# --- CONFIGURAÇÃO ---
HEADERS = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
ARQ_DADOS = 'dados.json'

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

# Se não houver data no ambiente, pega os últimos 3 dias
if not env_inicio:
    hoje = datetime.now()
    env_inicio = (hoje - timedelta(days=3)).strftime('%Y%m%d')
    env_fim = hoje.strftime('%Y%m%d')

dict_novos = {}
data_atual = datetime.strptime(env_inicio, '%Y%m%d')
data_final = datetime.strptime(env_fim, '%Y%m%d')

print(f"--- MODO DETETIVE ATIVADO: Buscando de {env_inicio} a {env_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n🔍 Verificando Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    while pagina <= 150:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        # Buscamos por DATA DE ATUALIZAÇÃO para pegar o que foi homologado hoje, mesmo que publicado meses atrás
        params = {
            "dataAtualizacaoInicial": DATA_STR, 
            "dataAtualizacaoFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", 
            "pagina": pagina, 
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            lics = resp.json().get('data', [])
            if not lics: break
            
            print("!", end="", flush=True) # Achou um lote

            for lic in lics:
                # Agora aceitamos mais status (4, 6, 8, 10) para garantir
                if str(lic.get('situacaoCompraId')) in ['4', '6', '8', '10']:
                    cnpj_org = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

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

# --- SALVAMENTO FORÇADO ---
historico = []
if os.path.exists(ARQ_DADOS):
    try:
        with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
            historico = json.load(f)
    except: pass

banco = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
print(f"\n\n📈 STATUS: {len(dict_novos)} novos resultados encontrados nesta rodada.")
banco.update(dict_novos)

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)

print(f"✅ FINALIZADO: Arquivo agora possui {len(banco)} registros.")
