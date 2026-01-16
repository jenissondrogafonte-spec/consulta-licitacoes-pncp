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

print(f"--- ROBÔ DE RESULTADOS EFETIVOS (POR DATA DE ATUALIZAÇÃO): {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Buscando resultados homologados em: {DATA_STR}", end=" ")
    
    pagina = 1
    while pagina <= 300: # Aumentado para pegar grandes volumes
        # ENDPOINT DE RESULTADOS (Este é o segredo para os 250k)
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/resultados"
        params = {
            "data": DATA_STR, # Busca por data do resultado/atualização
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            time.sleep(0.3)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            
            if resp.status_code != 200: break
            
            payload = resp.json()
            resultados = payload.get('data', [])
            if not resultados: break
            
            print(".", end="", flush=True)

            for res in resultados:
                # Filtramos apenas Pregão (6) conforme seu pedido
                if str(res.get('modalidadeId')) == "6":
                    
                    cnpj_orgao = res.get('cnpjOrgao')
                    ano = res.get('ano')
                    seq = res.get('sequencial')
                    uasg = str(res.get('codigoUnidadeOrgao', '000000')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"
                    
                    cnpj_venc = res.get('niFornecedor', '---')
                    chave = f"{id_lic}-{cnpj_venc}"
                    
                    # Se já processamos este vencedor nesta licitação, apenas somamos
                    if chave not in dict_resultados:
                        dict_resultados[chave] = {
                            "DataResult": res.get('dataAtualizacao', DATA_STR),
                            "UASG": uasg,
                            "Edital": f"{str(seq).zfill(5)}/{ano}",
                            "Orgao": res.get('razaoSocialOrgao'),
                            "UF": res.get('ufSiglaOrgao'),
                            "Municipio": res.get('municipioOrgao'),
                            "Fornecedor": res.get('nomeRazaoSocialFornecedor'),
                            "CNPJ": cnpj_venc,
                            "Licitacao": id_lic,
                            "Itens": []
                        }
                    
                    # Adiciona o item específico
                    dict_resultados[chave]["Itens"].append({
                        "Item": res.get('numeroItem'),
                        "Desc": res.get('descricaoItem'),
                        "Status": "Venceu",
                        "Valor": float(res.get('valorTotalHomologado') or 0)
                    })
            pagina += 1
        except Exception as e:
            print(f"[Erro: {e}]", end="")
            break
            
    data_atual += timedelta(days=1)

# SALVAMENTO
historico = []
if os.path.exists(ARQ_DADOS):
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass

historico.extend(list(dict_resultados.values()))
# Une itens de licitações/fornecedores repetidos
final_dict = {}
for item in historico:
    chave = f"{item['Licitacao']}-{item['CNPJ']}"
    if chave not in final_dict:
        final_dict[chave] = item
    else:
        # Se já existe, apenas mescla os itens sem duplicar
        existentes = {it['Item'] for it in final_dict[chave]['Itens']}
        for novo_it in item['Itens']:
            if novo_it['Item'] not in existentes:
                final_dict[chave]['Itens'].append(novo_it)

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_dict.values()), f, indent=4, ensure_ascii=False)

print(f"\n✅ Total de resultados salvos: {len(final_dict)}")
