import json
import os

ARQ_DADOS = 'dados.json'

def reparar_e_reorganizar_banco():
    if not os.path.exists(ARQ_DADOS):
        print(f"❌ Ficheiro {ARQ_DADOS} não encontrado.")
        return

    print("Leitura do banco de dados em curso...")
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        dados = json.load(f)

    total_original = len(dados)
    print(f"Total de registos encontrados: {total_original}")

    # --- 1. LIMPEZA (FILTRO DE ZEROS) ---
    # Mantém apenas os itens que possuem Quantidade E Valor Total maiores que zero
    dados_limpos = [
        item for item in dados 
        if float(item.get('Qtd') or 0) > 0 and float(item.get('Total') or 0) > 0
    ]
    
    total_removido = total_original - len(dados_limpos)
    print(f"🧹 Limpeza concluída: {total_removido} itens zerados (2º lugar ou sem saldo) foram removidos.")

    # --- 2. ORDENAÇÃO ESTÁVEL ---
    # 1º Passo: Ordena pelo Número do Item de forma CRESCENTE (1, 2, 3...)
    dados_limpos.sort(key=lambda x: int(x.get('Item', 0)) if str(x.get('Item', '')).isdigit() else 0)
    
    # 2º Passo: Agrupa pelo ID da Licitação (Decrescente)
    dados_limpos.sort(key=lambda x: x.get('Licitacao', ''), reverse=True)
    
    # 3º Passo: Ordena pela Data do Resultado (Mais recentes primeiro)
    dados_limpos.sort(key=lambda x: x.get('DataResult', x.get('DataPublicacao', '')), reverse=True)

    print("A guardar o banco de dados atualizado...")
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(dados_limpos, f, indent=4, ensure_ascii=False)

    print(f"✅ Ficheiro JSON reparado e reorganizado com sucesso! Total final: {len(dados_limpos)} itens.")

if __name__ == "__main__":
    reparar_e_reorganizar_banco()
