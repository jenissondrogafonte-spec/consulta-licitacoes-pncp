import requests
import json
from datetime import datetime, timedelta
import os
import time

# --- CONFIGURAÇÃO ---
HEADERS = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    hoje = datetime.now()
    d_ini = (hoje - timedelta(days=2)).strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

ARQ_DADOS = 'dados.json'
dict_resultados = {}

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ DE RESULTADOS DETALHADOS: {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    while pagina <= 100:
        # Buscamos compras publicadas na data
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
            if resp.status_code != 200: break
            
            licitacoes = resp.json().get('data', [])
            if not licitacoes: break
            
            print(".", end="", flush=True)

            for lic in licitacoes:
                # Pegamos apenas se estiver Homologada (4), Adjudicada (6) ou Encerrada (10)
                if str(lic.get('situacaoCompraId')) in ['4', '6', '10']:
                    cnpj_orgao = lic.get('orgaoEntidade', {}).get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    # Busca profunda dos ITENS para montar o Resumo (HML/FRC/DST)
                    try:
                        time.sleep(0.2)
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                        if r_it.status_code == 200:
                            itens_api = r_it.json()
                            
                            # Dicionário temporário para agrupar por fornecedor dentro deste pregão
                            fornecedores_neste_pregao = {}
                            resumo_pregao = {"Homologados": 0, "Fracassados": 0, "Desertos": 0}

                            for it in itens_api:
                                sit_nome = (it.get('situacaoItemNome') or "").upper()
                                
                                # Contabiliza resumo
                                if "FRACASSADO" in sit_nome: resumo_pregao["Fracassados"] += 1
                                elif "DESERTO" in sit_nome: resumo_pregao["Desertos"] += 1
                                
                                if it.get('temResultado'):
                                    resumo_pregao["Homologados"] += 1
                                    # Busca quem venceu este item
                                    r_v = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=10)
                                    if r_v.status_code == 200:
                                        vencedores = r_v.json()
                                        if isinstance(vencedores, dict): vencedores = [vencedores]
                                        for v in vencedores:
                                            cnpj_v = v.get('niFornecedor')
                                            if cnpj_v not in fornecedores_neste_pregao:
                                                fornecedores_neste_pregao[cnpj_v] = {
                                                    "DataResult": lic.get('dataAtualizacao'),
                                                    "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
                                                    "Orgao": lic.get('orgaoEntidade', {}).get('razaoSocial'),
                                                    "UF": lic.get('unidadeOrgao', {}).get('ufSigla'),
                                                    "Municipio": lic.get('unidadeOrgao', {}).get('municipioNome'),
                                                    "Fornecedor": v.get('nomeRazaoSocialFornecedor'),
                                                    "CNPJ": cnpj_v, "Licitacao": id_lic, "Itens": []
                                                }
                                            
                                            fornecedores_neste_pregao[cnpj_v]["Itens"].append({
                                                "Item": it.get('numeroItem'),
                                                "Desc": it.get('descricao'),
                                                "Qtd": v.get('quantidadeHomologada'),
                                                "Unitario": float(v.get('valorUnitarioHomologado') or 0),
                                                "Total": float(v.get('valorTotalHomologado') or 0),
                                                "Status": "Venceu"
                                            })
                            
                            # Salva os resultados no dicionário global
                            for f_cnpj, f_dados in fornecedores_neste_pregao.items():
                                f_dados["Resumo"] = resumo_pregao
                                dict_resultados[f"{id_lic}-{f_cnpj}"] = f_dados
                    except: pass
            pagina += 1
        except: break
    data_atual += timedelta(days=1)

# SALVAMENTO
historico = []
if os.path.exists(ARQ_DADOS):
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass

final_data = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
final_data.update(dict_resultados)

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_data.values()), f, indent=4, ensure_ascii=False)

print(f"\n✅ Concluído. Total no arquivo: {len(final_data)}")
