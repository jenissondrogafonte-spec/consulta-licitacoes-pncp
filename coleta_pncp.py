import requests
import json
from datetime import datetime, timedelta
import os
import sys

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

print(f"--- ROBÔ HÍBRIDO (COM DETALHE DE ITENS): {d_ini} até {d_fim} ---")

ARQ_VENCEDORES = 'dados.json'
ARQ_STATUS = 'status.json'

# Dicionários para agrupar dados automaticamente (chave única)
# Chave Vencedores: Licitacao + CNPJ_Fornecedor
dict_vencedores = {} 
lista_status = []
MAX_PAGINAS = 100 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while pagina <= MAX_PAGINAS:
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            "codigoModalidadeContratacao": "6", # Pregão
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code != 200: break
            
            licitacoes = resp.json().get('data', [])
            if not licitacoes: break
            
            print(f"[P{pagina}]", end=" ", flush=True)

            for lic in licitacoes:
                # --- DADOS ---
                cnpj_orgao = lic.get('orgaoEntidade', {}).get('cnpj')
                ano = lic.get('anoCompra')
                seq = lic.get('sequencialCompra')
                uasg = str(lic.get('unidadeOrgao', {}).get('codigoUnidade', '000000')).strip()
                nome_orgao = lic.get('orgaoEntidade', {}).get('razaoSocial', '')
                id_licitacao = f"{uasg}{str(seq).zfill(5)}{ano}" 
                numero_edital = f"{str(seq).zfill(5)}/{ano}"     
                objeto = lic.get('objetoCompra', 'Objeto não informado')
                situacao_nome = lic.get('situacaoCompraNome', 'Desconhecido')
                situacao_id = str(lic.get('situacaoCompraId'))
                data_abertura = lic.get('dataAberturaLicitacao', '')

                # 1. STATUS
                lista_status.append({
                    "DataPublicacao": DATA_STR,
                    "DataAbertura": data_abertura,
                    "UASG": uasg,
                    "Orgao": nome_orgao,
                    "Licitacao": id_licitacao,
                    "Numero": numero_edital,
                    "Objeto": objeto,
                    "Status": situacao_nome
                })

                # 2. VENCEDORES (Agrupamento com Detalhes)
                if situacao_id in ['4', '6']:
                    url_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens"
                    try:
                        r_it = requests.get(url_itens, headers=HEADERS, timeout=10)
                        if r_it.status_code == 200:
                            itens = r_it.json()
                            for it in itens:
                                if it.get('temResultado') is True:
                                    num_item = it.get('numeroItem')
                                    desc_item = it.get('descricao', 'Item sem descrição')
                                    qtd_item = it.get('quantidade', 1)
                                    
                                    # Busca Resultado Específico
                                    url_res = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}/itens/{num_item}/resultados"
                                    try:
                                        r_win = requests.get(url_res, headers=HEADERS, timeout=5)
                                        if r_win.status_code == 200:
                                            resultados = r_win.json()
                                            if isinstance(resultados, dict): resultados = [resultados]

                                            for res in resultados:
                                                cnpj_forn = res.get('niFornecedor')
                                                nome_forn = res.get('nomeRazaoSocialFornecedor')
                                                if not nome_forn and cnpj_forn: nome_forn = f"CNPJ {cnpj_forn}"
                                                
                                                valor = res.get('valorTotalHomologado')
                                                if valor is None: valor = 0
                                                valor = float(valor)

                                                if cnpj_forn:
                                                    chave = f"{id_licitacao}-{cnpj_forn}"
                                                    
                                                    # Se não existe no dicionário, cria
                                                    if chave not in dict_vencedores:
                                                        dict_vencedores[chave] = {
                                                            "Data": DATA_STR,
                                                            "UASG": uasg,
                                                            "Orgao": nome_orgao,
                                                            "Licitacao": id_licitacao,
                                                            "Fornecedor": nome_forn,
                                                            "CNPJ": cnpj_forn,
                                                            "Total": 0.0,
                                                            "Itens": 0,
                                                            "DetalhesItens": [] # Lista nova para guardar os itens
                                                        }
                                                    
                                                    # Adiciona valores e o detalhe do item
                                                    dict_vencedores[chave]["Total"] += valor
                                                    dict_vencedores[chave]["Itens"] += 1
                                                    dict_vencedores[chave]["DetalhesItens"].append({
                                                        "Item": num_item,
                                                        "Descricao": desc_item,
                                                        "Qtd": qtd_item,
                                                        "Valor": valor
                                                    })
                                    except: pass
                    except: pass
            pagina += 1
        except: break
    data_atual += timedelta(days=1)

# --- SALVAMENTO OTIMIZADO ---
def salvar_arquivo_json(nome_arquivo, dados_novos):
    if not dados_novos: return
    historico = []
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            try: historico = json.load(f)
            except: historico = []
    
    # Adiciona novos dados
    historico.extend(dados_novos)
    
    # Remove duplicatas convertendo para string JSON (set) e voltando
    # (Isso funciona para o Status)
    if nome_arquivo == ARQ_STATUS:
         # Remove duplicatas baseado em chaves específicas
         unicos = {f"{i['Licitacao']}-{i['Status']}": i for i in historico}
         final = list(unicos.values())
    else:
        # Para Vencedores, usamos a chave única que criamos no dict
        # Se rodar 2x, ele substitui o antigo pelo novo (mais atualizado)
        # Mas aqui, como estamos apenas acumulando histórico, vamos simplificar:
        final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=4, ensure_ascii=False)
    print(f"💾 {nome_arquivo} atualizado! Total: {len(final)} registros.")

print("\n--- RESUMO ---")

# Transforma o dicionário de vencedores em lista
lista_vencedores = list(dict_vencedores.values())

if lista_vencedores:
    salvar_arquivo_json(ARQ_VENCEDORES, lista_vencedores)
else:
    print("⚠️ Nenhum vencedor novo capturado.")

if lista_status:
    salvar_arquivo_json(ARQ_STATUS, lista_status)
else:
    print("⚠️ Nenhum status novo capturado.")
