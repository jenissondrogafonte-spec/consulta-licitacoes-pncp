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

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Processando: {DATA_STR}", end=" ")
    
    pagina = 1
    while pagina <= 400:
        # Usando o endpoint de resultados consolidados do PNCP que espelha os dados do Compras.gov
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/resultados"
        params = {"data": DATA_STR, "pagina": pagina, "tamanhoPagina": 50}

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            data = resp.json().get('data', [])
            if not data: break
            print(".", end="", flush=True)

            for res in data:
                if str(res.get('modalidadeId')) == "6": # Pregão
                    uasg = str(res.get('codigoUnidadeOrgao')).strip()
                    seq = res.get('sequencial')
                    ano = res.get('ano')
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                    cnpj_venc = res.get('niFornecedor') or "SEM-VENCEDOR"
                    
                    # CHAVE: Licitacao + Fornecedor (Para agrupar itens do mesmo fornecedor no mesmo card)
                    chave = f"{id_lic}-{cnpj_venc}"
                    
                    if chave not in dict_resultados:
                        dict_resultados[chave] = {
                            "DataResult": res.get('dataResultadoPncp') or res.get('dataAtualizacao') or DATA_STR,
                            "UASG": uasg,
                            "Edital": f"{str(seq).zfill(5)}/{ano}",
                            "Orgao": res.get('razaoSocialOrgao'),
                            "UF": res.get('ufSiglaOrgao'),
                            "Municipio": res.get('municipioOrgao'),
                            "Fornecedor": res.get('nomeRazaoSocialFornecedor') or "ITENS FRACASSADOS/DESERTOS",
                            "CNPJ": cnpj_venc,
                            "Licitacao": id_lic,
                            "Resumo": {"Homologados": 0, "Fracassados": 0, "Desertos": 0},
                            "Itens": []
                        }
                    
                    # Detalhamento do Item baseado no Swagger enviado
                    sit_nome = (res.get('situacaoCompraItemResultadoNome') or "").upper()
                    valor_total = float(res.get('valorTotalHomologado') or 0)
                    
                    item_info = {
                        "Item": res.get('numeroItem'),
                        "Desc": res.get('descricaoItem'),
                        "Qtd": res.get('quantidadeHomologada'),
                        "Unitario": float(res.get('valorUnitarioHomologado') or 0),
                        "Total": valor_total,
                        "Situacao": sit_nome
                    }
                    
                    # Contabilização para o Resumo do Pregão
                    if "FRACASSADO" in sit_nome: dict_resultados[chave]["Resumo"]["Fracassados"] += 1
                    elif "DESERTO" in sit_nome: dict_resultados[chave]["Resumo"]["Desertos"] += 1
                    else: 
                        dict_resultados[chave]["Resumo"]["Homologados"] += 1
                        dict_resultados[chave]["Itens"].append(item_info)

            pagina += 1
        except: break
    data_atual += timedelta(days=1)

# Persistência (Deduplicação Inteligente)
historico = []
if os.path.exists(ARQ_DADOS):
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass

# Mesclar novos dados
final_data = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
for k, v in dict_resultados.items():
    if k in final_data:
        # Atualiza itens se houver novos no mesmo fornecedor/pregão
        existentes = {it['Item'] for it in final_data[k]['Itens']}
        for novo in v['Itens']:
            if novo['Item'] not in existentes:
                final_data[k]['Itens'].append(novo)
    else:
        final_data[k] = v

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_data.values()), f, indent=4, ensure_ascii=False)
