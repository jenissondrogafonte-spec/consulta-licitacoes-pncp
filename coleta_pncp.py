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

UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    hoje = datetime.now()
    d_ini = (hoje - timedelta(days=1)).strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

MODALIDADE = "6" # Pregão Eletrônico
ARQ_DADOS = 'dados.json'
dict_resultados = {}

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ BRASIL TOTAL (POR UF): {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:")
    
    for uf in UFS:
        print(f"  UF {uf}:", end=" ", flush=True)
        pagina = 1
        tamanho_lote = 50 
        
        while pagina <= 150:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": DATA_STR, "dataFinal": DATA_STR,
                "codigoModalidadeContratacao": MODALIDADE,
                "uf": uf, # FILTRO POR ESTADO ADICIONADO
                "pagina": pagina, "tamanhoPagina": tamanho_lote
            }

            try:
                time.sleep(0.4)
                resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
                
                if resp.status_code != 200: break
                
                payload = resp.json()
                licitacoes = payload.get('data', [])
                if not licitacoes: break
                
                print(".", end="", flush=True)

                for lic in licitacoes:
                    sit_id = str(lic.get('situacaoCompraId'))
                    # 4:Homologada, 6:Adjudicada, 10:Encerrada
                    if sit_id in ['4', '6', '10']:
                        orgao = lic.get('orgaoEntidade', {})
                        unidade = lic.get('unidadeOrgao', {})
                        cnpj = orgao.get('cnpj')
                        ano = lic.get('anoCompra')
                        seq = lic.get('sequencialCompra')
                        uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                        id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                        data_res = lic.get('dataAtualizacao') or lic.get('dataPublicacaoPncp')
                        
                        # Captura Detalhes dos Itens
                        try:
                            r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=12)
                            if r_it.status_code == 200:
                                itens_api = r_it.json()
                                for it in itens_api:
                                    sit_it_nome = (it.get('situacaoItemNome') or "").upper()
                                    
                                    # VENCEDOR
                                    if it.get('temResultado'):
                                        r_w = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{it.get('numeroItem')}/resultados", headers=HEADERS, timeout=8)
                                        if r_w.status_code == 200:
                                            vends = r_w.json()
                                            if isinstance(vends, dict): vends = [vends]
                                            for v in vends:
                                                cnpj_v = v.get('niFornecedor', '00000000000000')
                                                chave = f"{id_lic}-{cnpj_v}"
                                                if chave not in dict_resultados:
                                                    dict_resultados[chave] = {
                                                        "DataResult": data_res, "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
                                                        "Orgao": orgao.get('razaoSocial'), "UF": uf,
                                                        "Municipio": unidade.get('municipioNome'), 
                                                        "Fornecedor": v.get('nomeRazaoSocialFornecedor'),
                                                        "CNPJ": cnpj_v, "Licitacao": id_lic, "Itens": []
                                                    }
                                                dict_resultados[chave]["Itens"].append({
                                                    "Item": it.get('numeroItem'), "Desc": it.get('descricao'), "Status": "Venceu",
                                                    "Valor": float(v.get('valorTotalHomologado') or 0)
                                                })
                                    # FRACASSADO
                                    elif "FRACASSADO" in sit_it_nome or "DESERTO" in sit_it_nome:
                                        chave = f"{id_lic}-SEM-VENC"
                                        if chave not in dict_resultados:
                                            dict_resultados[chave] = {
                                                "DataResult": data_res, "UASG": uasg, "Edital": f"{str(seq).zfill(5)}/{ano}",
                                                "Orgao": orgao.get('razaoSocial'), "UF": uf,
                                                "Municipio": unidade.get('municipioNome'), "Fornecedor": "⚠️ ITEM FRACASSADO/DESERTO",
                                                "CNPJ": "---", "Licitacao": id_lic, "Itens": []
                                            }
                                        dict_resultados[chave]["Itens"].append({
                                            "Item": it.get('numeroItem'), "Desc": it.get('descricao'), "Status": sit_it_nome.capitalize(), "Valor": 0
                                        })
                        except: pass
                pagina += 1
            except: break
        print(f" OK")
    data_atual += timedelta(days=1)

# SALVAMENTO
historico = []
if os.path.exists(ARQ_DADOS):
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass
historico.extend(list(dict_resultados.values()))
final_dict = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_dict.values()), f, indent=4, ensure_ascii=False)
print(f"\n✅ Total final: {len(final_dict)}")
