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

# MODALIDADES ALVO
MODALIDADES_ALVO = ["6", "1"] # Pregão e Dispensa
NOMES_MODALIDADE = {"6": "Pregão", "1": "Dispensa"}

print(f"--- ROBÔ DE ALTA CAPACIDADE (200 PÁGINAS): {d_ini} até {d_fim} ---")

ARQ_VENCEDORES = 'dados.json'
ARQ_STATUS = 'status.json'

dict_vencedores = {} 
lista_status = []

# DEFINIDO PARA 200 PARA GARANTIR O VOLUME TOTAL (Já que reduzimos o peso por página)
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
            
            # 20 itens por página = Mais páginas, mas ZERO travamentos
            params = {
                "dataInicial": DATA_STR,
                "dataFinal": DATA_STR,
                "codigoModalidadeContratacao": cod_mod,
                "pagina": pagina,
                "tamanhoPagina": 20 
            }

            sucesso = False
            # TENTA ATÉ 3 VEZES SE DER ERRO
            for tentativa in range(3):
                try:
                    # Delay inteligente
                    sleep_time = 0.2 if tentativa == 0 else 2.0
                    time.sleep(sleep_time) 
                    
                    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
                    
                    if resp.status_code == 200:
                        sucesso = True
                        break 
                    elif resp.status_code == 404:
                        sucesso = True 
                        break
                    else:
                        print(f"[E{resp.status_code}]", end="", flush=True)
                except:
                    print(f"[TCP]", end="", flush=True)
            
            if not sucesso:
                print(f"[FALHA]", end="", flush=True)
                break 
            
            try:
                payload = resp.json()
                licitacoes = payload.get('data', [])
            except:
                licitacoes = []

            if not licitacoes: 
                break # Fim das páginas deste dia
            
            # Visualização compacta
            if pagina == 1: print(f"[{nome_mod}]", end=" ", flush=True)
            else: print(f".", end="", flush=True)

            for lic in licitacoes:
                # --- EXTRAÇÃO ---
                orgao = lic.get('orgaoEntidade', {})
                unidade = lic.get('unidadeOrgao', {})
                cnpj_orgao = orgao.get('cnpj')
                nome_orgao = orgao.get('razaoSocial', '')
                uf = unidade.get('ufSigla') or orgao.get('ufSigla') or "BR"
                cidade = unidade.get('municipioNome') or "Não Informado"

                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(unidade.get('codigoUnidade', '000000')).strip()
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}" 
                numero_edital = f"{str(seq).zfill(5)}/{ano}"     
                objeto = lic.get('objetoCompra', 'Objeto não informado')
                situacao_nome = lic.get('situacaoCompraNome', 'Desconhecido')
                situacao_id = str(lic.get('situacaoCompraId'))
                modalidade_nome = lic.get('modalidadeAmparoNome', 'Desconhecida')

                # --- DATAS ---
                dt_abertura = lic.get('dataAberturaLicitacao') 
                dt_encerramento = lic.get('dataEncerramentoProposta')
                
                # Deep Fetch Otimizado
                if not dt_abertura and not dt_encerramento:
                    for _ in range(2):
                        try:
                            time.sleep(0.5)
                            url_full = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}"
                            r_full = requests.get(url_full, headers=HEADERS, timeout=5)
                            if r_full.status_code == 200:
                                detalhe = r_full.json()
                                dt_abertura = detalhe.get('dataAberturaLicitacao')
                                dt_encerramento = detalhe.get('dataEncerramentoProposta')
                                break
                        except: pass
                
                data_final_exibicao = dt_encerramento if dt_encerramento else dt_abertura
                if not data_final_exibicao: data_final_exibicao = ""

                # --- STATUS INTELIGENTE ---
                status_calculado = situacao_nome
                if situacao_id in ['4']:
                    status_calculado = "Homologada"
                elif situacao_id == '1' or "Divulgada" in situacao_nome:
                    if data_final_exibicao:
                        try:
                            dt_licitacao = datetime.fromisoformat(data_final_exibicao)
                            if dt_licitacao < datetime.now():
                                status_calculado = "Aguardando Resultado"
                            else:
                                status_calculado = "Recebendo Propostas"
                        except: pass

                # 1. STATUS
                lista_status.append({
                    "DataPublicacao": DATA_STR,
                    "DataAbertura": data_final_exibicao,
                    "UASG": uasg,
                    "Orgao": nome_orgao,
                    "UF": uf,
                    "Cidade": cidade,
                    "Licitacao": id_licitacao,
                    "Numero": numero_edital,
                    "Modalidade": modalidade_nome,
                    "Objeto": objeto,
                    "Status": status_calculado
                })

                # 2. VENCEDORES
                if situacao_id in ['4', '6']:
                    itens = []
                    # Tenta pegar itens
                    for _ in range(2):
                        try:
                            time.sleep(0.5)
                            url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens"
                            r_it = requests.get(url_itens, headers=HEADERS, timeout=10)
                            if r_it.status_code == 200:
                                itens = r_it.json()
                                break
                        except: pass

                    for it in itens:
                        if it.get('temResultado') is True:
                            num_item = it.get('numeroItem')
                            desc_item = it.get('descricao', 'Item sem descrição')
                            qtd_item = it.get('quantidade', 1)
                            
                            # Tenta pegar resultados
                            resultados = []
                            for _ in range(2):
                                try:
                                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens/{num_item}/resultados"
                                    r_win = requests.get(url_res, headers=HEADERS, timeout=5)
                                    if r_win.status_code == 200:
                                        res_json = r_win.json()
                                        if isinstance(res_json, dict): resultados = [res_json]
                                        else: resultados = res_json
                                        break
                                except: pass

                            for res in resultados:
                                cnpj_forn = res.get('niFornecedor')
                                nome_forn = res.get('nomeRazaoSocialFornecedor')
                                if not nome_forn and cnpj_forn: nome_forn = f"CNPJ {cnpj_forn}"
                                valor = float(res.get('valorTotalHomologado') or 0)

                                if cnpj_forn:
                                    chave = f"{id_licitacao}-{cnpj_forn}"
                                    if chave not in dict_vencedores:
                                        dict_vencedores[chave] = {
                                            "Data": DATA_STR,
                                            "UASG": uasg,
                                            "Orgao": nome_orgao,
                                            "UF": uf,
                                            "Cidade": cidade,
                                            "Licitacao": id_licitacao,
                                            "Numero": numero_edital,
                                            "Fornecedor": nome_forn,
                                            "CNPJ": cnpj_forn,
                                            "Total": 0.0,
                                            "Itens": 0,
                                            "DetalhesItens": []
                                        }
                                    dict_vencedores[chave]["Total"] += valor
                                    dict_vencedores[chave]["Itens"] += 1
                                    dict_vencedores[chave]["DetalhesItens"].append({
                                        "Item": num_item,
                                        "Descricao": desc_item,
                                        "Qtd": qtd_item,
                                        "Valor": valor
                                    })
            pagina += 1
            
    data_atual += timedelta(days=1)

# --- SALVAMENTO ---
def salvar_arquivo_json(nome_arquivo, dados_novos):
    if not dados_novos: return
    historico = []
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            try: historico = json.load(f)
            except: historico = []
    
    historico.extend(dados_novos)
    
    if nome_arquivo == ARQ_VENCEDORES:
        dict_unico = {}
        for item in historico:
            chave = f"{item.get('Licitacao')}-{item.get('CNPJ')}"
            dict_unico[chave] = item
        final = list(dict_unico.values())
    elif nome_arquivo == ARQ_STATUS:
         dict_unico = {}
         for item in historico:
             chave = item.get('Licitacao')
             dict_unico[chave] = item
         final = list(dict_unico.values())
    else:
        final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=4, ensure_ascii=False)
    print(f"\n💾 {nome_arquivo} atualizado! Total: {len(final)} registros.")

print("\n--- RESUMO DA COLETA ---")
lista_vencedores = list(dict_vencedores.values())
if lista_vencedores: salvar_arquivo_json(ARQ_VENCEDORES, lista_vencedores)
else: print("⚠️ Nenhum vencedor novo encontrado nesta execução.")
if lista_status: salvar_arquivo_json(ARQ_STATUS, lista_status)
else: print("⚠️ Nenhum status novo encontrado nesta execução.")
