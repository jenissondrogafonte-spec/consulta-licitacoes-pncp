import json
import requests
import time
import os

# --- CONFIGURAÇÃO ---
ARQ_DADOS = 'dados.json'
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
CNPJ_ALVO = "08778201000126"

def carregar_dados():
    if not os.path.exists(ARQ_DADOS):
        print("❌ Arquivo dados.json não encontrado.")
        return []
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_dados(dados):
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print("💾 Dados salvos (checkpoint)!")

def atualizar():
    dados = carregar_dados()
    total = len(dados)
    print(f"🔄 Iniciando atualização V2 (via ID PNCP) de {total} registros...\n")

    for i, licitacao in enumerate(dados):
        orgao_nome = licitacao.get('Orgao', 'Desconhecido')[:30] # Pega só o começo do nome
        print(f"[{i+1}/{total}] {orgao_nome}...", end=" ")

        # ESTRATÉGIA SEGURA: Usar o ID PNCP para montar a URL
        id_pncp = licitacao.get('IdPNCP')
        
        if not id_pncp:
            print("⚠️ Sem ID PNCP, pulando.")
            continue

        try:
            # O ID tem o formato: CNPJ-MODALIDADE-SEQUENCIAL/ANO
            # Ex: 12345678000199-1-00005/2025
            partes_hifen = id_pncp.split('-')
            cnpj_org = partes_hifen[0]
            
            # Pega a última parte (00005/2025) e divide pela barra
            resto = partes_hifen[-1] 
            seq = resto.split('/')[0]
            ano = resto.split('/')[1]
        except:
            print(f"⚠️ Erro ao ler ID: {id_pncp}")
            continue

        # 1. Buscar Datas de Propostas (Detalhes da Compra)
        try:
            url_detalhes = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}"
            # print(f"(URL: {url_detalhes})", end="") # Debug se precisar
            resp = requests.get(url_detalhes, headers=HEADERS, timeout=10)
            
            if resp.status_code == 200:
                detalhes = resp.json()
                licitacao['DtInicioPropostas'] = detalhes.get('dataInicioRecebimentoPropostas')
                licitacao['DtFimPropostas'] = detalhes.get('dataFimRecebimentoPropostas')
                print("✅ Prazos", end=" | ")
            else:
                print(f"❌ HTTP {resp.status_code} no Edital", end=" | ")
        except Exception as e:
            print(f"❌ Erro Conexão: {e}", end=" | ")

        # 2. Buscar Data Real de Homologação (Iterar sobre itens ganhos)
        item_atualizado = False
        data_homologacao_real = None

        if 'Itens' in licitacao:
            for item in licitacao['Itens']:
                try:
                    time.sleep(0.3) # Um pouco mais devagar para não bloquear
                    num_item = item['Item']
                    url_resultado = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}/itens/{num_item}/resultados"
                    
                    resp_res = requests.get(url_resultado, headers=HEADERS, timeout=10)
                    if resp_res.status_code == 200:
                        resultados = resp_res.json()
                        if isinstance(resultados, dict): resultados = [resultados]
                        
                        for res in resultados:
                            # Verifica se é o nosso CNPJ
                            cv = (res.get('niFornecedor') or "").replace(".", "").replace("/", "").replace("-", "")
                            if CNPJ_ALVO in cv:
                                nova_data = res.get('dataResultado')
                                if nova_data:
                                    data_homologacao_real = nova_data
                                    item_atualizado = True
                                    break
                        if item_atualizado: break # Se achou data em um item, já serve
                except:
                    pass
        
        if item_atualizado and data_homologacao_real:
            licitacao['DataResult'] = data_homologacao_real
            print("✅ Data Homologação")
        else:
            print("⚠️ Data Mantida")

        # Salva a cada 10 registros
        if (i + 1) % 10 == 0:
            salvar_dados(dados)

    salvar_dados(dados)
    print("\n✨ Atualização V2 concluída! Verifique o index.html.")

if __name__ == "__main__":
    atualizar()
