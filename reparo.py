import json
import os

ARQ_DADOS = 'dados.json'

def reorganizar_banco():
    if not os.path.exists(ARQ_DADOS):
        print(f"❌ Ficheiro {ARQ_DADOS} não encontrado.")
        return

    print("Leitura do banco de dados em curso...")
    with open(ARQ_DADOS, 'r', encoding='utf-8') as f:
        dados = json.load(f)

    print(f"Total de registos encontrados: {len(dados)}")

    # Ordenação estável em 3 etapas (a prioridade define-se do fim para o início):
    
    # 1º Passo: Ordena pelo Número do Item de forma CRESCENTE (1, 2, 3...)
    dados.sort(key=lambda x: int(x.get('Item', 0)) if str(x.get('Item', '')).isdigit() else 0)
    
    # 2º Passo: Agrupa pelo ID da Licitação (Decrescente)
    dados.sort(key=lambda x: x.get('Licitacao', ''), reverse=True)
    
    # 3º Passo: Ordena pela Data do Resultado (Mais recentes primeiro)
    dados.sort(key=lambda x: x.get('DataResult', x.get('DataPublicacao', '')), reverse=True)

    print("A guardar o banco de dados reorganizado...")
    with open(ARQ_DADOS, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)

    print("✅ Ficheiro JSON reorganizado com sucesso!")

if __name__ == "__main__":
    reorganizar_banco()
