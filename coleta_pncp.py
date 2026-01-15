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

MODALIDADE = "6" # Apenas Pregão Eletrônico
ARQ_DADOS = 'dados.json'

dict_resultados = {}
data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

print(f"--- ROBÔ INTELIGENTE (ADAPTATIVO 50/20): {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    tamanho_lote = 50 # Começa rápido
    
    while pagina <= 200:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": MODALIDADE,
            "pagina": pagina, "tamanhoPagina": tamanho_lote
        }

        try:
            time.sleep(0.5)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
            
            # SE DER ERRO COM LOTE DE 50, TENTA DIMINUIR PARA 20
            if resp.status_code in [400, 422] and tamanho_lote == 50:
                print("[Reduzindo Lote para 20...]", end="")
                tamanho_lote = 20
                pagina = 1 # Reinicia o dia com lote menor para garantir consistência
                continue
            
            if resp.status_code != 200: break
            
            payload = resp.json()
            licitacoes = payload.get('data', [])
            if not licitacoes: break
            
            print("+" if tamanho_lote == 50 else ".", end="", flush=True)

            for lic in licitacoes:
                sit_id = str(lic.get('situacaoCompraId'))
                # Captura resultados: Homologada (4), Adjudicada (6), Encerrada (10)
                if sit_id in ['4', '6', '10']:
                    orgao = lic.get('orgaoEntidade', {})
                    unidade = lic.get('unidadeOrgao', {})
                    cnpj = orgao.get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    edital = f"{str(seq).zfill(5)}/{ano}"
                    ini_rec = lic.get('dataInicioRecebimentoProposta', '')
                    fim_rec = lic.get('dataEncerramentoProposta', '')
                    # Data final de referência para o filtro do site
                    data_res = lic.get('dataAtualizacao') or lic.get('dataPublicacaoPncp')

                    try:
                        time.sleep(0.2)
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                        if r_it.status_code == 200:
                            for it in r_it.json():
                                n_it = it.get('numeroItem')
                                desc = it.get('descricao', 'Sem descrição')
                                sit_it_nome = (it.get('situacaoItemNome') or "").upper()
                                
                                # CASO 1: VENCEDOR
                                if it.get('temResultado'):
                                    r_w = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{n_it}/resultados", headers=HEADERS, timeout=10)
                                    if r_w.status_code == 200:
                                        vends = r_w.json()
                                        if isinstance(vends, dict): vends = [vends]
                                        for v in vends:
                                            cnpj_v = v.get('niFornecedor', '00000000000000')
                                            chave = f"{id_lic}-{cnpj_v}"
                                            if chave not in dict_resultados:
                                                dict_resultados[chave] = {
                                                    "DataResult": data_res, "UASG": uasg, "Edital": edital,
                                                    "Orgao": orgao.get('razaoSocial'), "UF": unidade.get('ufSigla'),
                                                    "Municipio": unidade.get('municipioNome'), 
                                                    "Fornecedor": v.get('nomeRazaoSocialFornecedor'),
                                                    "CNPJ": cnpj_v, "InicioRec": ini_rec, "FimRec": fim_rec,
                                                    "Licitacao": id_lic, "Itens": []
                                                }
                                            dict_resultados[chave]["Itens"].append({
                                                "Item": n_it, "Desc": desc, "Status": "Venceu",
                                                "Valor": float(v.get('valorTotalHomologado') or 0)
                                            })
                                
                                # CASO 2: FRACASSADO / DESERTO
                                elif any(s in sit_it_nome for s in ["FRACASSADO", "DESERTO"]):
                                    chave = f"{id_lic}-SEM-VENC"
                                    if chave not in dict_resultados:
                                        dict_resultados[chave] = {
                                            "DataResult": data_res, "UASG": uasg, "Edital": edital,
                                            "Orgao": orgao.get('razaoSocial'), "UF": unidade.get('ufSigla'),
                                            "Municipio": unidade.get('municipioNome'), 
                                            "Fornecedor": "⚠️ ITEM FRACASSADO/DESERTO",
                                            "CNPJ": "---", "InicioRec": ini_rec, "FimRec": fim_rec,
                                            "Licitacao": id_lic, "Itens": []
                                        }
                                    dict_resultados[chave]["Itens"].append({
                                        "Item": n_it, "Desc": desc, "Status": sit_it_nome.capitalize(), "Valor": 0
                                    })
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

historico.extend(list(dict_resultados.values()))
final_dict = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_dict.values()), f, indent=4, ensure_ascii=False)

print(f"\n✅ Total no arquivo: {len(final_dict)}")
