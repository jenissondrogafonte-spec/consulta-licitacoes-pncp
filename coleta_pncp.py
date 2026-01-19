import requests
import json
from datetime import datetime, timedelta
import os
import time
import urllib3

# Desativa avisos de SSL para evitar erros no Windows/Ambiente local
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
ARQ_DADOS = 'dados.json'
ARQ_CHECKPOINT = 'checkpoint.txt'
CNPJ_ALVO = "08778201000126"
DATA_LIMITE_FINAL = datetime(2025, 12, 31)

def carregar_banco():
    if os.path.exists(ARQ_DADOS):
        try:
            with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                return {f"{i['Licitacao']}-{i['CNPJ']}": i for i in dados}
        except: pass
    return {}

def salvar_estado(banco, data_proxima):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(list(banco.values()), f, indent=4, ensure_ascii=False)
    with open(ARQ_CHECKPOINT, 'w') as f:
        f.write(data_proxima.strftime('%Y%m%d'))
    print(f"\n💾 Checkpoint salvo: {data_proxima.strftime('%d/%m/%Y')}")

def buscar_detalhes_compra(cnpj, ano, sequencial):
    """ Busca datas de início/fim e o ID oficial no cabeçalho da compra """
    seq_formatado = str(sequencial).zfill(6)
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq_formatado}"
    try:
        r = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                "id_pncp": f"{cnpj}-1-{seq_formatado}/{ano}",
                "inicio": data.get('dataAberturaProposta') or data.get('dataInicioRecebimentoPropostas'),
                "fim": data.get('dataEncerramentoProposta') or data.get('dataFimRecebimentoPropostas'),
                "link": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq_formatado}"
            }
    except: pass
    return None

# --- INÍCIO DO PROCESSO ---
banco_total = carregar_banco()
data_atual = datetime.now() - timedelta(days=1) # Começa de ontem por padrão

if os.path.exists(ARQ_CHECKPOINT):
    with open(ARQ_CHECKPOINT, 'r') as f:
        data_atual = datetime.strptime(f.read().strip(), '%Y%m%d')

print(f"🚀 Iniciando coleta de resultados para o CNPJ {CNPJ_ALVO}...")

while data_atual <= DATA_LIMITE_FINAL:
    data_str = data_atual.strftime('%Y%m%d')
    print(f"\n📅 Data: {data_atual.strftime('%d/%m/%Y')}", end=" ", flush=True)
    
    pagina = 1
    while True:
        url_res = f"https://pncp.gov.br/api/pncp/v1/resultados/fornecedor/{CNPJ_ALVO}?dataSfi={data_str}&dataSff={data_str}&pagina={pagina}&tamanhoPagina=50"
        
        try:
            resp = requests.get(url_res, headers=HEADERS, verify=False, timeout=15)
            if resp.status_code != 200: break
            
            json_resp = resp.json()
            itens = json_resp.get('data', [])
            if not itens: break

            for it in itens:
                try:
                    # Dados básicos do item
                    cnpj_orgao = it.get('orgaoCnpj')
                    ano_compra = it.get('anoCompra')
                    seq_compra = it.get('sequencialCompra')
                    id_lic = f"{cnpj_orgao}{it.get('numeroCompra')}{ano_compra}"
                    chave = f"{id_lic}-{CNPJ_ALVO}"

                    # Se a licitação ainda não está no banco, buscamos os detalhes (Datas e ID)
                    if chave not in banco_total:
                        detalhes = buscar_detalhes_compra(cnpj_orgao, ano_compra, seq_compra)
                        
                        banco_total[chave] = {
                            "DataResult": it.get('dataInclusao'),
                            "DtInicioPropostas": detalhes['inicio'] if detalhes else None,
                            "DtFimPropostas": detalhes['fim'] if detalhes else None,
                            "IdPNCP": detalhes['id_pncp'] if detalhes else None,
                            "Link": detalhes['link'] if detalhes else f"https://pncp.gov.br/app/editais/{cnpj_orgao}/{ano_compra}/{seq_compra}",
                            "UASG": str(it.get('unidadeCompradora', {}).get('codigoUnidade', '')),
                            "Edital": it.get('numeroCompra'),
                            "Orgao": it.get('orgaoRazaoSocial'),
                            "UF": it.get('unidadeCompradora', {}).get('ufSigla'),
                            "Municipio": it.get('unidadeCompradora', {}).get('municipioNome'),
                            "Fornecedor": it.get('nomeRazaoSocialFornecedor'),
                            "CNPJ": CNPJ_ALVO,
                            "Licitacao": id_lic,
                            "Itens": []
                        }

                    # Adiciona o item específico vencido
                    if not any(x['Item'] == it.get('numeroItem') for x in banco_total[chave]["Itens"]):
                        banco_total[chave]["Itens"].append({
                            "Item": it.get('numeroItem'),
                            "Desc": it.get('descricaoItem'),
                            "Qtd": it.get('quantidadeHomologada'),
                            "Unitario": float(it.get('valorUnitarioHomologado') or 0),
                            "Total": float(it.get('valorTotalHomologado') or 0),
                            "Status": "Venceu"
                        })
                    print("🎯", end="", flush=True)
                except: continue
            
            if pagina >= json_resp.get('totalPaginas', 1): break
            pagina += 1
        except: break
    
    # Salva o estado ao fim de cada dia processado
    salvar_estado(banco_total, data_atual + timedelta(days=1))
    data_atual += timedelta(days=1)
    time.sleep(0.5) # Pausa amigável para o servidor

print(f"\n\n✅ Ciclo concluído. Verifique o arquivo {ARQ_DADOS}")
