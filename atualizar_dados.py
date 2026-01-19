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
    print("💾 Dados salvos com sucesso!")

def atualizar():
    dados = carregar_dados()
    total = len(dados)
    print(f"🔄 Iniciando atualização de {total} registros existentes...\n")

    for i, licitacao in enumerate(dados):
        print(f"[{i+1}/{total}] Atualizando {licitacao['Orgao']}...", end=" ")

        # Extrair dados do Link (formato: https://pncp.gov.br/app/editais/CNPJ/ANO/SEQ)
        try:
            parts = licitacao['Link'].split('/')
            cnpj_org = parts[-3]
            ano = parts[-2]
            seq = parts[-1]
        except:
            print("⚠️ Erro ao ler link")
            continue

        # 1. Buscar Datas de Propostas (Detalhes da Compra)
        try:
            url_detalhes = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_org}/compras/{ano}/{seq}"
            resp = requests.get(url_detalhes, headers=HEADERS, timeout=10)
            
            if resp.status_code == 200:
                detalhes = resp.json()
                licitacao['DtInicioPropostas'] = detalhes.get('dataInicioRecebimentoPropostas')
                licitacao['DtFimPropostas'] = detalhes.get('dataFimRecebimentoPropostas')
                print("✅ Datas Prazos", end=" | ")
            else:
                print("❌ Falha Prazos", end=" | ")
        except:
            print("❌ Erro Conexão Prazos", end=" | ")

        # 2. Buscar Data Real de Homologação (Iterar sobre itens ganhos)
        item_atualizado = False
        data_homologacao_real = None

        for item in licitacao['Itens']:
            try:
                # Pequeno delay para não bloquear a API
                time.sleep(0.2) 
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
            except:
                pass
        
        if item_atualizado and data_homologacao_real:
            licitacao['DataResult'] = data_homologacao_real
            print("✅ Data Homologação")
        else:
            print("⚠️ Mantida Data Antiga")

        # Salva a cada 5 registros para garantir
        if (i + 1) % 5 == 0:
            salvar_dados(dados)

    salvar_dados(dados)
    print("\n✨ Atualização completa! Agora rode o coleta_pncp.py normalmente para novos dias.")

if __name__ == "__main__":
    atualizar()
