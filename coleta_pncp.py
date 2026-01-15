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
    inicio = hoje - timedelta(days=2)
    d_ini = inicio.strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

# FOCO EXCLUSIVO: Pregão Eletrônico (ID 6)
MODALIDADE = "6"
ARQ_RESULTADOS = 'dados.json'

dict_resultados = {}
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ DE RESULTADOS (APENAS PREGÃO): {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    while pagina <= 200:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": MODALIDADE,
            "pagina": pagina, "tamanhoPagina": 20
        }

        try:
            time.sleep(0.5)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
            if resp.status_code != 200: break
            
            data = resp.json().get('data', [])
            if not data: break
            
            print(".", end="", flush=True)

            for lic in data:
                situacao_id = str(lic.get('situacaoCompraId'))
                # Focamos em licitações que já possuem resultado (Homologada/Encerrada)
                if situacao_id in ['4', '6', '10']: # 4: Homologada, 6: Adjudicada, 10: Encerrada
                    orgao = lic.get('orgaoEntidade', {})
                    unidade = lic.get('unidadeOrgao', {})
                    cnpj = orgao.get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    # Captura de datas e edital
                    edital = f"{str(seq).zfill(5)}/{ano}"
                    ini_rec = lic.get('dataInicioRecebimentoProposta', '')
                    fim_rec = lic.get('dataEncerramentoProposta', '')
                    ult_atu = lic.get('dataAtualizacao', DATA_STR)

                    # Busca de Itens (Vencedores, Fracassados e Desertos)
                    try:
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=10)
                        if r_it.status_code == 200:
                            itens = r_it.json()
                            for it in itens:
                                num_item = it.get('numeroItem')
                                desc_item = it.get('descricao')
                                status_item = it.get('situacaoItemNome') # Fracassado, Deserto, Homologado
                                
                                # Se o item tem resultado (Vencedor)
                                if it.get('temResultado'):
                                    r_w = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{num_item}/resultados", headers=HEADERS, timeout=5)
                                    if r_w.status_code == 200:
                                        vencedores = r_w.json()
                                        if isinstance(vencedores, dict): vencedores = [vencedores]
                                        for v in vencedores:
                                            cnpj_v = v.get('niFornecedor', '00000000000000')
                                            chave = f"{id_lic}-{cnpj_v}"
                                            
                                            if chave not in dict_resultados:
                                                dict_resultados[chave] = {
                                                    "DataResult": ult_atu, "UASG": uasg, "Edital": edital,
                                                    "Orgao": orgao.get('razaoSocial'), "UF": unidade.get('ufSigla'),
                                                    "Municipio": unidade.get('municipioNome'), "Fornecedor": v.get('nomeRazaoSocialFornecedor'),
                                                    "CNPJ": cnpj_v, "InicioRec": ini_rec, "FimRec": fim_rec,
                                                    "Licitacao": id_lic, "Itens": []
                                                }
                                            dict_resultados[chave]["Itens"].append({
                                                "Item": num_item, "Desc": desc_item, "Status": "Venceu",
                                                "Valor": float(v.get('valorTotalHomologado') or 0)
                                            })
                                # Se o item FRACASSOU ou restou DESERTO
                                elif any(s in status_item.upper() for s in ["FRACASSADO", "DESERTO"]):
                                    chave = f"{id_lic}-SEM-VENCEDOR"
                                    if chave not in dict_resultados:
                                        dict_resultados[chave] = {
                                            "DataResult": ult_atu, "UASG": uasg, "Edital": edital,
                                            "Orgao": orgao.get('razaoSocial'), "UF": unidade.get('ufSigla'),
                                            "Municipio": unidade.get('municipioNome'), "Fornecedor": "ITEM FRACASSADO/DESERTO",
                                            "CNPJ": "00.000.000/0000-00", "InicioRec": ini_rec, "FimRec": fim_rec,
                                            "Licitacao": id_lic, "Itens": []
                                        }
                                    dict_resultados[chave]["Itens"].append({
                                        "Item": num_item, "Desc": desc_item, "Status": status_item, "Valor": 0
                                    })
                    except: pass
            pagina += 1
        except: break
    data_atual += timedelta(days=1)

# Salvamento com deduplicação
historico = []
if os.path.exists(ARQ_RESULTADOS):
    with open(ARQ_RESULTADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass
historico.extend(list(dict_resultados.values()))
dict_final = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
with open(ARQ_RESULTADOS, 'w', encoding='utf-8') as f:
    json.dump(list(dict_final.values()), f, indent=4, ensure_ascii=False)
print("\n✅ Concluído.")
