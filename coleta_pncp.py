<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Painel de Inteligência - Compras Públicas</title>
    
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">

    <style>
        :root { --gov-blue: #004494; --gov-light: #f8f9fa; --gov-accent: #2369B3; --gov-yellow: #FFD100; }
        body { background-color: #f4f7fa; font-family: 'Open Sans', sans-serif; color: #333; }
        
        .gov-header { background: var(--gov-blue); color: white; padding: 25px 0; border-bottom: 5px solid var(--gov-yellow); margin-bottom: 30px; box-shadow: 0 4px 10px rgba(0,0,0,0.2); }
        .main-container { background: white; border-radius: 8px; padding: 25px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
        .btn-nav-status { background-color: var(--gov-yellow); color: #333; font-weight: 700; border: none; padding: 8px 20px; border-radius: 5px; text-decoration: none; }
        
        /* Stats Cards */
        .stat-card { border: none; border-radius: 10px; padding: 20px; color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .bg-money { background: linear-gradient(135deg, #198754, #20c997); }
        .bg-items { background: linear-gradient(135deg, #0d6efd, #0dcaf0); }
        .bg-orgs { background: linear-gradient(135deg, #6f42c1, #a64bf4); }
        .stat-value { font-size: 2rem; font-weight: 700; margin: 0; }
        .stat-label { font-size: 0.85rem; text-transform: uppercase; font-weight: 600; }

        /* Filtros */
        .filter-section { background: var(--gov-light); padding: 25px; border-radius: 8px; margin-bottom: 25px; border-left: 5px solid var(--gov-blue); }
        .form-label { font-size: 0.75rem; font-weight: 800; color: #555; text-transform: uppercase; }
        .btn-search { background: var(--gov-blue); color: white; border: none; font-weight: 600; }
        
        /* Tabela */
        .table-custom { font-size: 0.9rem; border-radius: 8px; overflow: hidden; }
        .table-custom thead { background: #343a40; color: white; }
        .table-custom th { padding: 15px; }
        .table-custom td { padding: 12px 15px; vertical-align: middle; }
        
        /* Link do Valor */
        .valor-link { color: #198754; text-decoration: none; border-bottom: 1px dashed #198754; cursor: pointer; transition: 0.2s; }
        .valor-link:hover { background-color: #d1e7dd; color: #0f5132; }

        .empty-state { text-align: center; padding: 80px 20px; color: #adb5bd; border: 2px dashed #dee2e6; border-radius: 8px; }
    </style>
</head>
<body>

<header class="gov-header text-center">
    <div class="container">
        <h2 class="mb-1 fw-bold"><i class="fas fa-chart-line me-2"></i>Painel de Inteligência PNCP</h2>
        <p class="small mb-4 opacity-75">Monitoramento de Vencedores e Itens</p>
        <a href="status.html" class="btn-nav-status shadow"><i class="fas fa-search me-2"></i> Consultar Status e Objetos</a>
    </div>
</header>

<div class="container-fluid px-4 pb-5">
    <div class="main-container">
        
        <div class="row g-3 mb-4" id="rowIndicadores" style="display: none;">
            <div class="col-md-4">
                <div class="stat-card bg-money">
                    <p class="stat-label">Valor Total Homologado</p>
                    <p class="stat-value" id="valTotal">R$ 0,00</p>
                </div>
            </div>
            <div class="col-md-4">
                <div class="stat-card bg-items">
                    <p class="stat-label">Total de Itens</p>
                    <p class="stat-value" id="valItens">0</p>
                </div>
            </div>
            <div class="col-md-4">
                <div class="stat-card bg-orgs">
                    <p class="stat-label">Empresas Vencedoras</p>
                    <p class="stat-value" id="valEmpresas">0</p>
                </div>
            </div>
        </div>

        <div class="filter-section">
            <div class="row g-3 align-items-end">
                <div class="col-md-3">
                    <label class="form-label">Fornecedor / CNPJ</label>
                    <input type="text" id="inForn" class="form-control" placeholder="Nome ou CNPJ...">
                </div>
                <div class="col-md-3">
                    <label class="form-label">Órgão Público</label>
                    <input type="text" id="inOrg" class="form-control" placeholder="Prefeitura...">
                </div>
                <div class="col-md-2">
                    <label class="form-label">UASG</label>
                    <input type="text" id="inUasg" class="form-control" placeholder="926150">
                </div>
                <div class="col-md-2">
                    <label class="form-label">Licitação</label>
                    <input type="text" id="inLic" class="form-control" placeholder="Ano/Sequencial">
                </div>
                <div class="col-md-2 d-flex gap-2">
                    <button onclick="buscar()" class="btn btn-search w-100 py-2 shadow-sm">BUSCAR</button>
                    <button onclick="limpar()" class="btn btn-outline-secondary py-2"><i class="fas fa-eraser"></i></button>
                </div>
            </div>
        </div>

        <div id="resultadoContainer" style="display: none;">
            <div class="table-responsive">
                <table class="table table-hover table-custom border">
                    <thead>
                        <tr>
                            <th>DATA</th>
                            <th>UASG</th>
                            <th>ÓRGÃO</th>
                            <th>IDENTIFICAÇÃO</th>
                            <th>FORNECEDOR</th>
                            <th class="text-center">QTD ITENS</th>
                            <th class="text-end">VALOR TOTAL (R$)</th>
                        </tr>
                    </thead>
                    <tbody id="corpoTabela"></tbody>
                </table>
            </div>
        </div>

        <div id="estadoVazio" class="empty-state">
            <i class="fas fa-magnifying-glass-chart fa-4x mb-3 opacity-25"></i>
            <h5>Painel de Vencedores</h5>
            <p>Utilize os filtros acima para analisar quem está ganhando licitações.</p>
        </div>

    </div>
</div>

<div class="modal fade" id="modalItens" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-centered">
        <div class="modal-content">
            <div class="modal-header bg-primary text-white">
                <h5 class="modal-title"><i class="fas fa-list me-2"></i> Detalhes dos Itens Vencidos</h5>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <h6 id="modalTitulo" class="fw-bold mb-3 text-secondary"></h6>
                <div class="table-responsive">
                    <table class="table table-sm table-striped border">
                        <thead class="table-light">
                            <tr>
                                <th width="10%">Item</th>
                                <th width="60%">Descrição</th>
                                <th width="10%" class="text-center">Qtd</th>
                                <th width="20%" class="text-end">Valor Total</th>
                            </tr>
                        </thead>
                        <tbody id="modalTabelaBody"></tbody>
                    </table>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Fechar</button>
            </div>
        </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
<script>
    let dadosBanco = [];
    let resultadosAtuais = []; // Para guardar o resultado da busca atual

    fetch('dados.json?v=' + Date.now())
        .then(res => res.json())
        .then(data => { dadosBanco = data; })
        .catch(err => console.error("Sem dados ainda:", err));

    function buscar() {
        const fF = document.getElementById('inForn').value.toLowerCase().trim();
        const fO = document.getElementById('inOrg').value.toLowerCase().trim();
        const fU = document.getElementById('inUasg').value.trim();
        const fL = document.getElementById('inLic').value.trim();

        if(!fF && !fO && !fU && !fL) {
            alert("Preencha ao menos um campo.");
            return;
        }

        resultadosAtuais = dadosBanco.filter(i => {
            return ( (i.Fornecedor || '').toLowerCase().includes(fF) || (i.CNPJ || '').includes(fF) ) &&
                   ( (i.Orgao || '').toLowerCase().includes(fO) ) &&
                   ( (i.UASG || '').includes(fU) ) &&
                   ( (i.Licitacao || '').includes(fL) );
        });

        atualizarInterface(resultadosAtuais);
    }

    function atualizarInterface(lista) {
        const rowInd = document.getElementById('rowIndicadores');
        const resCont = document.getElementById('resultadoContainer');
        const vazio = document.getElementById('estadoVazio');
        const tbody = document.getElementById('corpoTabela');

        if (lista.length === 0) {
            alert("Nenhum registro encontrado.");
            return;
        }

        vazio.style.display = 'none';
        rowInd.style.display = 'flex';
        resCont.style.display = 'block';

        let totalDinheiro = 0;
        let totalItens = 0;
        let empresasUnicas = new Set();

        // Mapeia e cria a tabela
        tbody.innerHTML = lista.reverse().map((i, index) => {
            totalDinheiro += i.Total;
            totalItens += i.Itens;
            empresasUnicas.add(i.CNPJ);

            // AQUI ESTÁ A MÁGICA: O onclick chama a função abrirDetalhes passando o índice
            return `
                <tr>
                    <td>${formatarData(i.Data)}</td>
                    <td class="text-secondary small">${i.UASG || ''}</td>
                    <td>${i.Orgao || ''}</td>
                    <td><span class="badge bg-light text-primary border">${i.Licitacao.slice(6,11)}/${i.Licitacao.slice(11)}</span></td>
                    <td>
                        <div class="fw-bold text-dark">${i.Fornecedor || ''}</div>
                        <small class="text-muted" style="font-size:0.75rem">CNPJ: ${i.CNPJ || ''}</small>
                    </td>
                    <td class="text-center"><span class="badge bg-secondary">${i.Itens || '0'}</span></td>
                    <td class="text-end fw-bold">
                        <a onclick="abrirDetalhes(${index})" class="valor-link" title="Clique para ver os itens">
                            R$ ${Number(i.Total).toLocaleString('pt-BR', {minimumFractionDigits: 2})}
                        </a>
                    </td>
                </tr>
            `;
        }).join('');

        document.getElementById('valTotal').innerText = totalDinheiro.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'});
        document.getElementById('valItens').innerText = totalItens;
        document.getElementById('valEmpresas').innerText = empresasUnicas.size;
    }

    // Função que abre o Modal e preenche a tabela interna
    function abrirDetalhes(index) {
        // Como invertemos a lista na exibição (.reverse()), precisamos pegar o índice real
        // Truque: (Tamanho - 1) - indexVisual
        const indexReal = (resultadosAtuais.length - 1) - index;
        const item = resultadosAtuais[indexReal];
        
        // Preenche Titulo
        document.getElementById('modalTitulo').innerText = `${item.Fornecedor} - ${item.Orgao}`;
        
        // Preenche Tabela do Modal
        const tbodyModal = document.getElementById('modalTabelaBody');
        
        if (item.DetalhesItens && item.DetalhesItens.length > 0) {
            tbodyModal.innerHTML = item.DetalhesItens.map(detalhe => `
                <tr>
                    <td class="text-center fw-bold">${detalhe.Item}</td>
                    <td>${detalhe.Descricao}</td>
                    <td class="text-center">${detalhe.Qtd}</td>
                    <td class="text-end fw-bold text-success">
                        R$ ${Number(detalhe.Valor).toLocaleString('pt-BR', {minimumFractionDigits: 2})}
                    </td>
                </tr>
            `).join('');
        } else {
            tbodyModal.innerHTML = '<tr><td colspan="4" class="text-center text-muted">Detalhes dos itens não disponíveis para este registro antigo.</td></tr>';
        }

        // Abre o modal via Bootstrap
        var myModal = new bootstrap.Modal(document.getElementById('modalItens'));
        myModal.show();
    }

    function limpar() { location.reload(); }
    function formatarData(d) {
        if (!d || d.length !== 8) return d;
        return d.slice(6,8) + '/' + d.slice(4,6) + '/' + d.slice(0,4);
    }

    document.addEventListener('keypress', function (e) {
        if (e.key === 'Enter' && document.activeElement.tagName === 'INPUT') buscar();
    });
</script>

</body>
</html>
