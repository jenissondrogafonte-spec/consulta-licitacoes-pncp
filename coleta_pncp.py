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

# Definição de datas via variáveis de ambiente ou automático
env_inicio = os.getenv('DATA_INICIAL', '').strip()
env_fim = os.getenv('DATA_FINAL', '').strip()

if env_inicio and env_fim:
    d_ini, d_fim = env_inicio, env_fim
else:
    # MODO AUTOMÁTICO: Pega os últimos 3 dias
    hoje = datetime.now()
    inicio = hoje - timedelta(days=3)
    d_ini = inicio.strftime('%Y%m%d')
    d_fim = hoje.strftime('%Y%m%d')

print(f"--- ROBÔ UNIVERSAL (TODAS AS MODALIDADES): {d_ini} até {d_fim} ---")

ARQ_VENCEDORES = 'dados.json'
ARQ_STATUS = 'status.json'

# Estruturas de dados (Sem Pandas)
dict_vencedores = {} 
lista_status = []
MAX_PAGINAS = 200 

data_atual = datetime.strptime(d_ini, '%Y%m%d')
data_final = datetime.strptime(d_fim, '%Y%m%d')

while data_atual <= data_final:
    DATA_STR = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Dia {DATA_STR}:", end=" ")
    
    pagina = 1
    
    while pagina <= MAX_PAGINAS:
        # URL de busca pública
        url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
        params = {
            "dataInicial": DATA_STR,
            "dataFinal": DATA_STR,
            # REMOVIDO FILTRO DE MODALIDADE: Agora traz Pregão, Dispensa, Inexigibilidade, etc.
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        try:
            # Pequeno delay para não sobrecarregar a API
            time.sleep(0.3)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
            
            if resp.status_code != 200: break
            
            payload = resp.json()
            licitacoes = payload.get('data', [])
            
            if not licitacoes: break
            
            print(f"[P{pagina}]", end=" ", flush=True)

            for lic in licitacoes:
                # --- EXTRAÇÃO DE DADOS ---
                orgao = lic.get('orgaoEntidade', {})
                unidade = lic.get('unidadeOrgao', {})
                
                cnpj_orgao = orgao.get('cnpj')
                nome_orgao = orgao.get('razaoSocial', '')
                # Captura UF e Cidade para os filtros
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

                # --- LÓGICA DE DATAS (SOLICITAÇÃO ATENDIDA) ---
                # Prioridade: Data de Fim de Recebimento de Proposta. Se não houver, usa Abertura.
                dt_abertura = lic.get('dataAberturaLicitacao') 
                dt_encerramento = lic.get('dataEncerramentoProposta')
                
                # Se ambas vazias, tenta busca profunda (Deep Fetch)
                if not dt_abertura and not dt_encerramento:
                    try:
                        url_full = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano}/{seq}"
                        r_full = requests.get(url_full, headers=HEADERS, timeout=5)
                        if r_full.status_code == 200:
                            detalhe = r_full.json()
                            dt_abertura = detalhe.get('dataAberturaLicitacao')
                            dt_encerramento = detalhe.get('dataEncerramentoProposta')
                    except: pass
                
                # Regra: Considerar Abertura = Fim do Recebimento (se existir)
                data_final_exibicao = dt_encerramento if dt_encerramento else dt_abertura
                if not data_final_exibicao: data_final_exibicao = ""

                # --- 1. POPULA LISTA DE STATUS ---
                lista_status.append({
                    "DataPublicacao": DATA_STR,
                    "DataAbertura": data_final_exibicao,
                    "UASG": uasg,
                    "Orgao": nome_orgao,
                    "UF": uf,           # Novo Campo
                    "Cidade": cidade,   # Novo Campo
                    "Licitacao": id_licitacao,
                    "Numero": numero_edital,
                    "Modalidade": modalidade_nome,
                    "Objeto": objeto,
                    "Status": situacao_nome
                })

                # --- 2. POPULA VENCEDORES (DICIONÁRIO INTELIGENTE) ---
                # Só processa se tiver resultado (Homologada/Adjudicada)
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
                                                    # CHAVE ÚNICA: Licitação + Fornecedor
                                                    chave = f"{id_licitacao}-{cnpj_forn}"
                                                    
                                                    # Se é a primeira vez que vemos esse fornecedor nessa licitação, cria a estrutura
                                                    if chave not in dict_vencedores:
                                                        dict_vencedores[chave] = {
                                                            "Data": DATA_STR,
                                                            "UASG": uasg,
                                                            "Orgao": nome_orgao,
                                                            "UF": uf,
                                                            "Cidade": cidade,
                                                            "Licitacao": id_licitacao, # ID interno
                                                            "Numero": numero_edital,   # Visual (00001/2025)
                                                            "Fornecedor": nome_forn,
                                                            "CNPJ": cnpj_forn,
                                                            "Total": 0.0,
                                                            "Itens": 0,
                                                            "DetalhesItens": [] # Lista que acumula os itens
                                                        }
                                                    
                                                    # AGREGANDO VALORES (Sem Pandas)
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
        except Exception as e:
            # Em caso de erro de conexão, tenta continuar o loop
            break
            
    data_atual += timedelta(days=1)

# --- FUNÇÃO DE SALVAMENTO ---
def salvar_arquivo_json(nome_arquivo, dados_novos):
    if not dados_novos: return
    historico = []
    
    # Lê arquivo existente se houver
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            try: historico = json.load(f)
            except: historico = []
    
    historico.extend(dados_novos)
    
    # Remove duplicatas
    if nome_arquivo == ARQ_STATUS:
         # Chave única para status: Licitação + Status atual
         unicos = {f"{i['Licitacao']}-{i['Status']}": i for i in historico}
         final = list(unicos.values())
    else:
        # Para vencedores, usamos a serialização para garantir unicidade
        final = [json.loads(x) for x in list(set([json.dumps(i, sort_keys=True) for i in historico]))]

    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=4, ensure_ascii=False)
    print(f"💾 {nome_arquivo} atualizado! Total: {len(final)} registros.")

print("\n--- RESUMO DA COLETA ---")
# Converte o dicionário em lista para salvar
lista_vencedores = list(dict_vencedores.values())

if lista_vencedores:
    salvar_arquivo_json(ARQ_VENCEDORES, lista_vencedores)
else:
    print("⚠️ Nenhum vencedor novo encontrado.")

if lista_status:
    salvar_arquivo_json(ARQ_STATUS, lista_status)
else:
    print("⚠️ Nenhum status novo encontrado.")
