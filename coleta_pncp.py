import requests
import json
from datetime import datetime, timedelta
import os
import sys
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
    inicio = hoje - timedelta(days=3)
    d_ini = inicio.strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

MODALIDADES_ALVO = ["6", "1"] 
NOMES_MODALIDADE = {"6": "Pregão", "1": "Dispensa"}

print(f"--- ROBÔ RESILIENTE (TRATAMENTO E204): {d_ini} até {d_fim} ---")

ARQ_VENCEDORES = 'dados.json'
ARQ_STATUS = 'status.json'

dict_vencedores = {} 
lista_status = []
MAX_PAGINAS = 200 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    for cod_mod in MODALIDADES_ALVO:
        nome_mod = NOMES_MODALIDADE.get(cod_mod, cod_mod)
        pagina = 1
        
        while pagina <= MAX_PAGINAS:
            url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            params = {
                "dataInicial": DATA_STR,
                "dataFinal": DATA_STR,
                "codigoModalidadeContratacao": cod_mod,
                "pagina": pagina,
                "tamanhoPagina": 20 
            }

            sucesso = False
            vazio = False
            
            for tentativa in range(3):
                try:
                    time.sleep(0.5 if tentativa == 0 else 2.0)
                    resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
                    
                    if resp.status_code == 200:
                        sucesso = True
                        break
                    elif resp.status_code == 204:
                        # CORREÇÃO AQUI: Se for 204, avisamos que está vazio e paramos as páginas
                        sucesso = True
                        vazio = True
                        break
                    elif resp.status_code == 404:
                        sucesso = True
                        vazio = True
                        break
                    else:
                        print(f"[E{resp.status_code}]", end="", flush=True)
                except:
                    print(f"[TCP]", end="", flush=True)
            
            if not sucesso or vazio:
                break # Pula para a próxima modalidade ou dia

            try:
                payload = resp.json()
                licitacoes = payload.get('data', [])
            except:
                licitacoes = []

            if not licitacoes: break
            
            if pagina == 1: print(f"[{nome_mod}]", end=" ", flush=True)
            else: print(f".", end="", flush=True)

            for lic in licitacoes:
                orgao = lic.get('orgaoEntidade', {})
                unidade = lic.get('unidadeOrgao', {})
                cnpj_orgao = orgao.get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}" 
                numero_edital = f"{str(seq).zfill(5)}/{ano}"     
                objeto = lic.get('objetoCompra', 'Objeto não informado')
                situacao_nome = lic.get('situacaoCompraNome', 'Desconhecido')
                situacao_id = str(lic.get('situacaoCompraId'))
                
                dt_abertura = lic.get('dataAberturaLicitacao') 
                dt_encerramento = lic.get('dataEncerramentoProposta')
                
                # Deep Fetch
                if not dt_abertura and not dt_encerramento:
                    try:
                        r_f = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}", headers=HEADERS, timeout=5)
                        if r_f.status_code == 200:
                            det = r_f.json()
                            dt_abertura = det.get('dataAberturaLicitacao')
                            dt_encerramento = det.get('dataEncerramentoProposta')
                    except: pass
                
                data_final_exibicao = dt_encerramento if dt_encerramento else dt_abertura
                
                # Status Calculado
                status_calc = situacao_nome
                if situacao_id == '4': status_calc = "Homologada"
                elif situacao_id == '1' or "Divulgada" in situacao_nome:
                    if data_final_exibicao:
                        try:
                            if datetime.fromisoformat(data_final_exibicao) < datetime.now():
                                status_calc = "Aguardando Resultado"
                            else:
                                status_calc = "Recebendo Propostas"
                        except: pass

                lista_status.append({
                    "DataPublicacao": DATA_STR,
                    "DataAbertura": data_final_exibicao,
                    "UASG": uasg,
                    "Orgao": orgao.get('razaoSocial', ''),
                    "UF": unidade.get('ufSigla') or orgao.get('ufSigla') or "BR",
                    "Cidade": unidade.get('municipioNome') or "Não Informado",
                    "Licitacao": id_licitacao,
                    "Numero": numero_edital,
                    "Objeto": objeto,
                    "Status": status_calc
                })

                # VENCEDORES
                if situacao_id in ['4', '6']:
                    try:
                        r_it = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens", headers=HEADERS, timeout=10)
                        if r_it.status_code == 200:
                            for it in r_it.json():
                                if it.get('temResultado'):
                                    n_it = it.get('numeroItem')
                                    r_w = requests.get(f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens/{n_it}/resultados", headers=HEADERS, timeout=5)
                                    if r_w.status_code == 200:
                                        res_list = r_w.json()
                                        if isinstance(res_list, dict): res_list = [res_list]
                                        for res in res_list:
                                            cnpj_f = res.get('niFornecedor')
                                            if cnpj_f:
                                                chave = f"{id_licitacao}-{cnpj_f}"
                                                if chave not in dict_vencedores:
                                                    dict_vencedores[chave] = {
                                                        "Data": DATA_STR, "UASG": uasg, "Orgao": orgao.get('razaoSocial', ''),
                                                        "Licitacao": id_licitacao, "Numero": numero_edital,
                                                        "Fornecedor": res.get('nomeRazaoSocialFornecedor') or f"CNPJ {cnpj_f}",
                                                        "CNPJ": cnpj_f, "Total": 0.0, "Itens": 0, "DetalhesItens": []
                                                    }
                                                val = float(res.get('valorTotalHomologado') or 0)
                                                dict_vencedores[chave]["Total"] += val
                                                dict_vencedores[chave]["Itens"] += 1
                                                dict_vencedores[chave]["DetalhesItens"].append({"Item": n_it, "Descricao": it.get('descricao'), "Qtd": it.get('quantidade'), "Valor": val})
                    except: pass
            pagina += 1
    data_atual += timedelta(days=1)

def salvar_arquivo_json(nome_arquivo, dados_novos):
    if not dados_novos: return
    historico = []
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            try: historico = json.load(f)
            except: historico = []
    historico.extend(dados_novos)
    if nome_arquivo == ARQ_VENCEDORES:
        dict_un = {f"{i['Licitacao']}-{i['CNPJ']}": i for i in historico}
        final = list(dict_un.values())
    else:
        dict_un = {i['Licitacao']: i for i in historico}
        final = list(dict_un.values())
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=4, ensure_ascii=False)

lista_vencedores = list(dict_vencedores.values())
salvar_arquivo_json(ARQ_VENCEDORES, lista_vencedores)
salvar_arquivo_json(ARQ_STATUS, lista_status)
print("\n💾 Processo concluído com sucesso.")
