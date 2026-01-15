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

# Datas via GitHub Actions ou Automático
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

print(f"--- ROBÔ DE RESULTADOS: {d_ini} até {d_fim} ---")

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    while pagina <= 150:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR, "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": MODALIDADE,
            "pagina": pagina, "tamanhoPagina": 20
        }

        try:
            time.sleep(0.6)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
            
            # Trata erros de API (204, 400, 422) como "fim de dados" para não travar
            if resp.status_code != 200:
                print(f"[Fim: {resp.status_code}]", end="")
                break
            
            payload = resp.json()
            licitacoes = payload.get('data', [])
            if not licitacoes: break
            
            print(".", end="", flush=True)

            for lic in licitacoes:
                # Pegamos licitações com resultado (ID 4:Homologada, 6:Adjudicada, 10:Encerrada)
                sit_id = str(lic.get('situacaoCompraId'))
                if sit_id in ['4', '6', '10']:
                    orgao = lic.get('orgaoEntidade', {})
                    unidade = lic.get('unidadeOrgao', {})
                    cnpj = orgao.get('cnpj')
                    ano = lic.get('anoCompra')
                    seq = lic.get('sequencialCompra')
                    uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                    id_lic = f"{uasg}{str(seq).zfill(5)}{ano}"

                    # Dados exigidos para o novo filtro
                    edital = f"{str(seq).zfill(5)}/{ano}"
                    ini_rec = lic.get('dataInicioRecebimentoProposta', '')
                    fim_rec = lic.get('dataEncerramentoProposta', '')
                    # Data do Resultado ou Última Atualização
                    data_res = lic.get('dataAtualizacao') or lic.get('dataPublicacaoPncp') or DATA_STR

                    # Busca profunda de itens
                    try:
                        time.sleep(0.3)
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=15)
                        if r_it.status_code == 200:
                            itens_api = r_it.json()
                            for it in itens_api:
                                n_it = it.get('numeroItem')
                                desc = it.get('descricao', 'Sem descrição')
                                sit_it_nome = (it.get('situacaoItemNome') or "").upper()
                                
                                # CASO 1: TEM VENCEDOR
                                if it.get('temResultado'):
                                    r_w = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{n_it}/resultados", headers=HEADERS, timeout=10)
                                    if r_w.status_code == 200:
                                        vends = r_w.json()
                                        if isinstance(vends, dict): vends = [vends]
                                        for v in vends:
                                            cnpj_v = v.get('niFornecedor', '00.000.000/0000-00')
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
                                
                                # CASO 2: FRACASSADO OU DESERTO
                                elif "FRACASSADO" in sit_it_nome or "DESERTO" in sit_it_nome:
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
        except Exception as e:
            print(f"[Erro: {e}]", end="")
            break
    data_atual += timedelta(days=1)

# SALVAMENTO SEGURO
historico = []
if os.path.exists(ARQ_DADOS):
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        try: historico = json.load(f)
        except: pass

historico.extend(list(dict_resultados.values()))
# Deduplicação por Licitação + Fornecedor
final_dict = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}

with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
    json.dump(list(final_dict.values()), f, indent=4, ensure_ascii=False)

print(f"\n✅ Concluído! Total no arquivo: {len(final_dict)} registros.")
