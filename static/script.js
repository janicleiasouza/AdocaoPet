"use strict";

/*
 * PetAmigo, lado do aparelho.
 *
 * Cada celular guarda uma cópia (réplica) do estado do servidor:
 *   1. ao abrir ou reconectar, baixa o estado completo (/api/estado);
 *   2. depois aplica os eventos que chegam pelo WebSocket, na ordem do seq;
 *   3. se faltar um evento por muito tempo, baixa o estado completo de novo.
 * Toda requisição leva o relógio de Lamport do aparelho.
 */

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------------------
// Identidade do aparelho
// ---------------------------------------------------------------------------

const memoria = {
    ler(chave) { try { return localStorage.getItem(chave); } catch { return null; } },
    gravar(chave, valor) { try { localStorage.setItem(chave, valor); } catch { /* modo privado */ } },
};

let clienteId = memoria.ler("petamigo.cliente");
if (!clienteId) {
    clienteId = "c-" + Math.random().toString(36).slice(2, 10);
    memoria.gravar("petamigo.cliente", clienteId);
}
let meuNome = memoria.ler("petamigo.nome") || "";

// ---------------------------------------------------------------------------
// Réplica local
// ---------------------------------------------------------------------------

const replica = {
    animais: new Map(),   // id -> animal
    pedidos: new Map(),   // id -> pedido
    fila: new Map(),      // animal_id -> [{cliente_id, nome}] em ordem de chegada
    seq: 0,               // último evento aplicado
};

let duracaoReserva = 60;
let lamport = 0;
let diferencaRelogio = 0;      // relógio do servidor - relógio do aparelho
let ressincronizacoes = 0;
let socket = null;
let sincronizando = false;
let recebidosNaSincronizacao = [];
const foraDeOrdem = new Map(); // seq -> evento que chegou antes do anterior
let esperaPorLacuna = null;

let filtroEspecie = "";
let textoBusca = "";
let pedidoAberto = null;       // id do animal no diálogo de pedido

const agoraServidor = () => Date.now() / 1000 + diferencaRelogio;

// ---------------------------------------------------------------------------
// Utilidades
// ---------------------------------------------------------------------------

function esc(valor) {
    return String(valor ?? "").replace(/[&<>"']/g, (c) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

function icone(nome, classe = "ic") {
    return `<svg class="${classe}" aria-hidden="true"><use href="#i-${nome}"/></svg>`;
}

function iniciais(nome) {
    return String(nome || "?").trim().split(/\s+/).slice(0, 2).map((p) => p[0]).join("").toUpperCase();
}

function primeiroNome(nome) {
    return String(nome || "").trim().split(/\s+/)[0];
}

function segundosRestantes(animal) {
    return Math.max(0, Math.ceil((animal.reservado_ate || 0) - agoraServidor()));
}

function formatarHora(epoch, comSegundos = true) {
    const opcoes = { hour: "2-digit", minute: "2-digit" };
    if (comSegundos) opcoes.second = "2-digit";
    return new Date(epoch * 1000).toLocaleTimeString("pt-BR", opcoes);
}

function linkWhatsApp(numero) {
    let digitos = String(numero || "").replace(/\D/g, "");
    if (!digitos) return null;
    if (digitos.length <= 11) digitos = "55" + digitos;
    return `https://wa.me/${digitos}`;
}

function aviso(texto, tipo = "info", duracao = 3800) {
    const el = document.createElement("div");
    el.className = "aviso " + tipo;
    el.textContent = texto;
    $("avisos").appendChild(el);
    setTimeout(() => el.classList.add("saindo"), duracao);
    setTimeout(() => el.remove(), duracao + 400);
}

// Notificação do sistema (só funciona em HTTPS e com a aba em segundo plano).
const podeNotificar = "Notification" in window && window.isSecureContext;

function pedirPermissaoDeNotificacao() {
    if (podeNotificar && Notification.permission === "default") {
        Notification.requestPermission().catch(() => {});
    }
}

function alertar(texto, tipo = "destaque") {
    aviso(texto, tipo, 6000);
    try { navigator.vibrate?.([180, 80, 180]); } catch { /* sem vibração */ }
    if (podeNotificar && Notification.permission === "granted" && document.hidden) {
        try { new Notification("PetAmigo", { body: texto, tag: "petamigo" }); } catch { /* ignorado */ }
    }
}

// ---------------------------------------------------------------------------
// Requisições (todas levam o relógio de Lamport)
// ---------------------------------------------------------------------------

async function chamar(caminho, corpo = {}) {
    lamport += 1; // evento local: envio de mensagem
    let resposta;
    try {
        resposta = await fetch(caminho, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ cliente_id: clienteId, nome: meuNome, lamport, ...corpo }),
        });
    } catch {
        throw new Error("Sem conexão com o servidor. Tente de novo.");
    }
    const dados = await resposta.json().catch(() => ({}));
    if (typeof dados.lamport === "number") lamport = Math.max(lamport, dados.lamport) + 1;
    atualizarMetricas();
    if (!resposta.ok) {
        throw new Error(typeof dados.detail === "string" ? dados.detail : "Não foi possível concluir. Tente de novo.");
    }
    return dados;
}

// ---------------------------------------------------------------------------
// Consistência: estado completo + eventos em ordem
// ---------------------------------------------------------------------------

async function ressincronizar(motivo) {
    if (sincronizando) return;
    sincronizando = true;
    foraDeOrdem.clear();
    clearTimeout(esperaPorLacuna);
    esperaPorLacuna = null;
    try {
        const resposta = await fetch("/api/estado", { cache: "no-store" });
        if (!resposta.ok) throw new Error();
        const estado = await resposta.json();

        diferencaRelogio = estado.agora - Date.now() / 1000;
        duracaoReserva = estado.duracao_reserva;
        replica.animais = new Map(estado.animais.map((a) => [a.id, a]));
        replica.pedidos = new Map(estado.pedidos.map((p) => [p.id, p]));
        replica.fila = new Map();
        for (const pessoa of estado.fila) {
            if (!replica.fila.has(pessoa.animal_id)) replica.fila.set(pessoa.animal_id, []);
            replica.fila.get(pessoa.animal_id).push(pessoa);
        }
        replica.seq = estado.seq;
        lamport = Math.max(lamport, estado.lamport) + 1;

        $("registro").innerHTML = "";
        estado.eventos.slice().reverse().forEach((e) => adicionarAoRegistro(e, false));

        if (motivo) {
            ressincronizacoes += 1;
            aviso(`Dados atualizados (${motivo}).`);
        }
        sincronizando = false;

        // eventos que chegaram enquanto o estado era baixado
        const atrasados = recebidosNaSincronizacao;
        recebidosNaSincronizacao = [];
        atrasados.forEach(receberEvento);
    } catch {
        sincronizando = false;
        setTimeout(() => ressincronizar(motivo || "nova tentativa"), 2000);
    }
    renderizar();
}

function aplicarVersao(mapa, registro) {
    if (!registro) return;
    const local = mapa.get(registro.id);
    if (!local || registro.versao >= local.versao) mapa.set(registro.id, registro);
}

function receberEvento(evento) {
    if (sincronizando) { recebidosNaSincronizacao.push(evento); return; }
    if (evento.seq <= replica.seq) return; // repetido ou antigo

    foraDeOrdem.set(evento.seq, evento);
    while (foraDeOrdem.has(replica.seq + 1) && !sincronizando) {
        const proximo = foraDeOrdem.get(replica.seq + 1);
        foraDeOrdem.delete(proximo.seq);
        aplicarEvento(proximo);
    }

    // Faltou algum evento: espera um pouco (pode estar só atrasado) e depois baixa tudo.
    if (foraDeOrdem.size && !esperaPorLacuna) {
        esperaPorLacuna = setTimeout(() => {
            esperaPorLacuna = null;
            if (foraDeOrdem.size) ressincronizar(`evento ${replica.seq + 1} não chegou`);
        }, 1500);
    } else if (!foraDeOrdem.size && esperaPorLacuna) {
        clearTimeout(esperaPorLacuna);
        esperaPorLacuna = null;
    }
    renderizar();
}

function aplicarEvento(evento) {
    const dados = evento.dados || {};
    const eraMinhaFila = dados.animal && (replica.fila.get(dados.animal.id) || [])
        .some((p) => p.cliente_id === clienteId);

    replica.seq = evento.seq;
    lamport = Math.max(lamport, evento.lamport) + 1; // regra de recebimento de Lamport

    if (evento.evento === "demo_reiniciada") {
        adicionarAoRegistro(evento, true);
        if (evento.cliente !== clienteId) aviso(`${evento.autor} reiniciou a demonstração.`);
        ressincronizar();
        return;
    }

    aplicarVersao(replica.animais, dados.animal);
    aplicarVersao(replica.pedidos, dados.pedido);
    if (dados.animal && Array.isArray(dados.fila)) replica.fila.set(dados.animal.id, dados.fila);

    adicionarAoRegistro(evento, true);
    notificar(evento, eraMinhaFila);
}

// ---------------------------------------------------------------------------
// Comunicação: WebSocket
// ---------------------------------------------------------------------------

function mostrarConexao(estado, texto) {
    const el = $("conexao");
    el.className = "status-conexao " + estado;
    el.querySelector("span").textContent = texto;
}

function conectar() {
    const protocolo = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocolo}//${location.host}/ws?cliente=${encodeURIComponent(clienteId)}&nome=${encodeURIComponent(meuNome)}`;
    socket = new WebSocket(url);

    socket.onopen = () => {
        mostrarConexao("online", "Ao vivo");
        ressincronizar(replica.seq ? "reconectado" : null);
    };
    socket.onmessage = (mensagem) => {
        const dados = JSON.parse(mensagem.data);
        if (dados.tipo === "presenca") mostrarPresenca(dados);
        else if (dados.tipo === "evento") receberEvento(dados);
    };
    socket.onclose = () => {
        mostrarConexao("offline", "Reconectando");
        setTimeout(conectar, 2000);
    };
    socket.onerror = () => socket.close();
}

setInterval(() => { if (socket?.readyState === WebSocket.OPEN) socket.send("ping"); }, 20000);

function mostrarPresenca({ online, nomes }) {
    $("mOnline").textContent = online;
    $("nomesOnline").innerHTML = nomes.length
        ? nomes.map((n) => `<span>${esc(n)}</span>`).join("")
        : `<span class="vazio">Ninguém ainda</span>`;
}

// ---------------------------------------------------------------------------
// Navegação entre telas
// ---------------------------------------------------------------------------

const TELAS = ["inicio", "pedidos", "cadastrar", "aovivo"];

function rotaAtual() {
    const hash = location.hash.slice(1);
    const animal = hash.match(/^animal\/(\d+)$/);
    if (animal) return { tela: "animal", id: Number(animal[1]) };
    return { tela: TELAS.includes(hash) ? hash : "inicio" };
}

function mostrarTela() {
    const { tela } = rotaAtual();
    document.querySelectorAll(".tela").forEach((s) => { s.hidden = s.id !== "tela-" + tela; });
    document.querySelectorAll(".navegacao [data-tela]").forEach((link) => {
        const ativo = link.dataset.tela === tela || (tela === "animal" && link.dataset.tela === "inicio");
        link.classList.toggle("ativo", ativo);
        if (ativo) link.setAttribute("aria-current", "page"); else link.removeAttribute("aria-current");
    });
    document.body.classList.toggle("tela-detalhe-ativa", tela === "animal");
    window.scrollTo(0, 0);
    renderizar();
}

window.addEventListener("hashchange", mostrarTela);

// ---------------------------------------------------------------------------
// Situação de um animal do ponto de vista deste aparelho
// ---------------------------------------------------------------------------

const ROTULO_STATUS = {
    DISPONIVEL: "Disponível", RESERVADO: "Reservado", EM_ANALISE: "Em análise", ADOTADO: "Adotado",
};

function situacao(animal) {
    const fila = replica.fila.get(animal.id) || [];
    const posicao = fila.findIndex((p) => p.cliente_id === clienteId) + 1;
    const minhaVez = animal.reservado_por === clienteId;
    return {
        fila,
        posicao,
        minhaVez: minhaVez && animal.status === "RESERVADO",
        meuPedido: minhaVez && animal.status === "EM_ANALISE",
        adoteiEu: animal.status === "ADOTADO" && animal.adotado_cliente === clienteId,
    };
}

function fotoHtml(animal, classe, extra = "") {
    const inicial = esc((animal.nome || "?")[0].toUpperCase());
    const imagem = animal.foto
        ? `<img src="${esc(animal.foto)}" alt="${esc(animal.nome)}" loading="lazy"
               onerror="this.parentElement.classList.add('sem-foto'); this.remove()">`
        : "";
    return `<div class="${classe}${animal.foto ? "" : " sem-foto"}" data-inicial="${inicial}">${imagem}${extra}</div>`;
}

function etiquetaStatus(animal) {
    return `<span class="etiqueta ${animal.status.toLowerCase()}">${ROTULO_STATUS[animal.status]}</span>`;
}

function barraTempo(animal) {
    const pct = Math.min(100, (segundosRestantes(animal) / duracaoReserva) * 100);
    return `<div class="barra-tempo"><i data-prazo="${animal.reservado_ate}" style="width:${pct}%"></i></div>`;
}

// ---------------------------------------------------------------------------
// Tela inicial
// ---------------------------------------------------------------------------

function cardAnimal(animal) {
    const s = situacao(animal);
    let linha = "";
    if (s.minhaVez) {
        linha = `<p class="card-situacao minha">${icone("relogio")}Sua vez · <span data-prazo-texto="${animal.reservado_ate}">${segundosRestantes(animal)}</span>s</p>`;
    } else if (s.meuPedido) {
        linha = `<p class="card-situacao minha">${icone("check")}Seu pedido está em análise</p>`;
    } else if (s.posicao) {
        linha = `<p class="card-situacao">${icone("fila")}Você é o ${s.posicao}º da fila</p>`;
    } else if (s.adoteiEu) {
        linha = `<p class="card-situacao minha">${icone("casa")}Adotado por você</p>`;
    } else if (animal.status === "RESERVADO" || animal.status === "EM_ANALISE") {
        const espera = s.fila.length ? ` · ${s.fila.length} na fila` : "";
        linha = `<p class="card-situacao neutra">${icone("fila")}Com ${esc(primeiroNome(animal.reservado_nome))}${espera}</p>`;
    }

    return `
        <button class="card-animal ${animal.status.toLowerCase()}" data-abrir="${animal.id}">
            ${fotoHtml(animal, "foto", etiquetaStatus(animal))}
            <div class="card-corpo">
                <h3>${esc(animal.nome)}</h3>
                <p class="meta">${esc(animal.idade)} · ${esc(animal.sexo)} · ${esc(animal.porte)}</p>
                ${linha}
            </div>
        </button>`;
}

function renderizarInicio() {
    $("saudacao").textContent = meuNome ? `Olá, ${primeiroNome(meuNome)}` : "Olá!";
    document.querySelectorAll(".duracao").forEach((el) => { el.textContent = duracaoReserva; });

    const ordem = { DISPONIVEL: 0, RESERVADO: 1, EM_ANALISE: 2, ADOTADO: 3 };
    const busca = textoBusca.trim().toLowerCase();
    const animais = [...replica.animais.values()]
        .filter((a) => !filtroEspecie || a.especie === filtroEspecie)
        .filter((a) => !busca || a.nome.toLowerCase().includes(busca))
        .sort((a, b) => ordem[a.status] - ordem[b.status] || a.id - b.id);

    const disponiveis = animais.filter((a) => a.status !== "ADOTADO").length;
    $("contagemAnimais").textContent = disponiveis === 1 ? "1 animal" : `${disponiveis} animais`;
    $("listaAnimais").innerHTML = animais.length
        ? animais.map(cardAnimal).join("")
        : `<p class="vazio">Nenhum animal encontrado.</p>`;
}

// ---------------------------------------------------------------------------
// Detalhe do animal
// ---------------------------------------------------------------------------

function renderizarDetalhe(id) {
    const animal = replica.animais.get(id);
    const alvo = $("detalheAnimal");
    if (!animal) {
        alvo.innerHTML = replica.seq || replica.animais.size
            ? `<div class="detalhe-corpo"><p class="vazio">Animal não encontrado.</p><a class="botao botao-secundario botao-largo" href="#inicio">Voltar</a></div>`
            : `<p class="vazio">Carregando...</p>`;
        return;
    }
    const s = situacao(animal);
    const nome = esc(animal.nome);
    const sexoIcone = animal.sexo === "Fêmea" ? "femea" : "macho";

    let quadro = "";
    let acoes = "";
    if (animal.status === "ADOTADO") {
        if (s.adoteiEu) {
            const whats = linkWhatsApp(animal.whatsapp);
            quadro = `<div class="situacao-detalhe minha">Seu pedido foi aprovado. Combine a entrega com ${esc(animal.responsavel)}.</div>`;
            acoes = whats
                ? `<a class="botao botao-verde botao-grande botao-largo" href="${whats}" target="_blank" rel="noopener">${icone("whats")}Falar com o responsável</a>`
                : "";
        } else {
            quadro = `<div class="situacao-detalhe neutra">${nome} já foi adotado${animal.adotado_por ? ` por ${esc(primeiroNome(animal.adotado_por))}` : ""}.</div>`;
            acoes = `<button class="botao botao-secundario botao-grande botao-largo" disabled>Já adotado</button>`;
        }
    } else if (s.minhaVez) {
        quadro = `<div class="situacao-detalhe minha">A vez é sua. Envie o pedido em
            <b data-prazo-texto="${animal.reservado_ate}">${segundosRestantes(animal)}</b> segundos.${barraTempo(animal)}</div>`;
        acoes = `<button class="botao botao-primario botao-grande botao-largo" data-acao="pedir" data-id="${animal.id}">${icone("pata")}Enviar pedido</button>
                 <button class="botao botao-texto botao-largo" data-acao="desistir" data-id="${animal.id}">Desistir e passar a vez</button>`;
    } else if (s.meuPedido) {
        quadro = `<div class="situacao-detalhe minha">Seu pedido foi enviado. Você será avisado assim que ${esc(animal.responsavel)} decidir.</div>`;
        acoes = `<button class="botao botao-secundario botao-grande botao-largo" disabled>Aguardando resposta</button>`;
    } else if (s.posicao) {
        const frente = s.posicao - 1;
        quadro = `<div class="situacao-detalhe">Você é o <b>${s.posicao}º da fila</b>.
            ${frente ? `${frente} ${frente === 1 ? "pessoa" : "pessoas"} na sua frente, além de quem está com a vez.` : "Você é o próximo."}
            Vamos avisar quando chegar a sua vez.</div>`;
        acoes = `<button class="botao botao-secundario botao-grande botao-largo" disabled>${icone("fila")}Na fila</button>
                 <button class="botao botao-texto botao-largo" data-acao="desistir" data-id="${animal.id}">Sair da fila</button>`;
    } else if (animal.status === "DISPONIVEL") {
        acoes = `<button class="botao botao-primario botao-grande botao-largo" data-acao="reservar" data-id="${animal.id}">${icone("pata")}Quero adotar ${nome}</button>`;
    } else {
        const quem = esc(primeiroNome(animal.reservado_nome) || "outra pessoa");
        quadro = animal.status === "RESERVADO"
            ? `<div class="situacao-detalhe">${quem} está com a vez agora. Faltam
                   <b data-prazo-texto="${animal.reservado_ate}">${segundosRestantes(animal)}</b> segundos.
                   ${s.fila.length ? `${s.fila.length} na fila.` : ""}${barraTempo(animal)}</div>`
            : `<div class="situacao-detalhe">O pedido de ${quem} está em análise.
                   ${s.fila.length ? `${s.fila.length} na fila.` : ""}</div>`;
        acoes = `<button class="botao botao-primario botao-grande botao-largo" data-acao="reservar" data-id="${animal.id}">${icone("fila")}Entrar na fila</button>`;
    }

    alvo.innerHTML = `
        ${fotoHtml(animal, "detalhe-foto", `<a class="botao-redondo" href="#inicio" aria-label="Voltar">${icone("voltar")}</a>`)}
        <div class="detalhe-corpo">
            <div class="detalhe-titulo">
                <h1>${nome}</h1>
                ${etiquetaStatus(animal)}
            </div>
            <p class="detalhe-sub">${esc(animal.especie)} · ${esc(animal.idade)} · ${esc(animal.sexo)}</p>

            <div class="blocos-info">
                <div class="bloco-info">${icone(sexoIcone)}<b>${esc(animal.sexo)}</b><span>Sexo</span></div>
                <div class="bloco-info">${icone("bolo")}<b>${esc(animal.idade)}</b><span>Idade</span></div>
                <div class="bloco-info">${icone("porte")}<b>${esc(animal.porte)}</b><span>Porte</span></div>
            </div>

            ${quadro}
            <div class="acoes-detalhe">${acoes}</div>

            <h2>Sobre ${nome}</h2>
            <p class="sobre">${esc(animal.descricao)}</p>

            <div class="responsavel">
                <span class="avatar">${esc(iniciais(animal.responsavel))}</span>
                <div><small>Responsável</small><b>${esc(animal.responsavel)}</b></div>
            </div>
        </div>`;
}

// ---------------------------------------------------------------------------
// Pedidos
// ---------------------------------------------------------------------------

const ROTULO_PEDIDO = { PENDENTE: "Pendente", APROVADO: "Aprovado", RECUSADO: "Recusado" };

function cardPedido(p) {
    const animal = replica.animais.get(p.animal_id);
    const miniatura = animal?.foto
        ? `<img src="${esc(animal.foto)}" alt="" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'miniatura'}))">`
        : `<span class="miniatura"></span>`;
    const whats = linkWhatsApp(p.telefone);
    return `
        <article class="card-pedido">
            <div class="card-pedido-topo">
                ${miniatura}
                <div><h3>${esc(p.animal_nome)}</h3><p>Recebido às ${formatarHora(p.criado_em, false)}</p></div>
                <span class="etiqueta ${p.status.toLowerCase()}">${ROTULO_PEDIDO[p.status]}</span>
            </div>
            <dl class="dados-pedido">
                <div><dt>Interessado</dt><dd>${esc(p.adotante)}</dd></div>
                ${p.telefone ? `<div><dt>Contato</dt><dd>${whats ? `<a href="${whats}" target="_blank" rel="noopener">${esc(p.telefone)}</a>` : esc(p.telefone)}</dd></div>` : ""}
                ${p.mensagem ? `<div><dt>Mensagem</dt><dd>${esc(p.mensagem)}</dd></div>` : ""}
                ${p.decidido_por ? `<div><dt>Decidido por</dt><dd>${esc(p.decidido_por)}</dd></div>` : ""}
            </dl>
            ${p.status === "PENDENTE" ? `
            <div class="acoes-pedido">
                <button class="botao botao-secundario" data-acao="recusar" data-id="${p.id}" data-versao="${p.versao}">Recusar</button>
                <button class="botao botao-primario" data-acao="aprovar" data-id="${p.id}" data-versao="${p.versao}">${icone("check")}Aprovar</button>
            </div>` : ""}
            <p class="versao">versão ${p.versao}</p>
        </article>`;
}

function renderizarPedidos() {
    const todos = [...replica.pedidos.values()].sort((a, b) => b.id - a.id);
    const pendentes = todos.filter((p) => p.status === "PENDENTE");
    const decididos = todos.filter((p) => p.status !== "PENDENTE");

    const selo = $("seloPedidos");
    selo.hidden = !pendentes.length;
    selo.textContent = pendentes.length;

    if (!todos.length) {
        $("listaPedidos").innerHTML = `<p class="vazio">Nenhum pedido por enquanto.<br>Quando alguém pedir para adotar, o pedido aparece aqui.</p>`;
        return;
    }
    $("listaPedidos").innerHTML = `
        ${pendentes.length ? `<section class="grupo-pedidos"><h2>Aguardando decisão (${pendentes.length})</h2>
            <div class="lista-pedidos">${pendentes.map(cardPedido).join("")}</div></section>` : ""}
        ${decididos.length ? `<section class="grupo-pedidos"><h2>Histórico</h2>
            <div class="lista-pedidos">${decididos.map(cardPedido).join("")}</div></section>` : ""}`;
}

// ---------------------------------------------------------------------------
// Ao vivo
// ---------------------------------------------------------------------------

function renderizarAoVivo() {
    const select = $("animalCorrida");
    const atual = select.value;
    select.innerHTML = [...replica.animais.values()]
        .map((a) => `<option value="${a.id}">${esc(a.nome)} (${ROTULO_STATUS[a.status].toLowerCase()})</option>`)
        .join("");
    if (atual && replica.animais.has(Number(atual))) select.value = atual;
    atualizarMetricas();
}

function atualizarMetricas() {
    $("mLamport").textContent = lamport;
    $("mSeq").textContent = replica.seq;
    $("mResync").textContent = ressincronizacoes;
}

const DESCRICAO_EVENTO = {
    animal_cadastrado: (d) => `cadastrou ${d.animal.nome}`,
    vez_concedida: (d) => `pegou a vez de ${d.animal.nome}`,
    vez_negada: (d) => `teve a vez negada: ${d.motivo}`,
    fila_entrou: (d) => `entrou na fila de ${d.animal.nome} (${d.posicao}º)`,
    fila_saiu: (d) => `saiu da fila de ${d.animal.nome}`,
    vez_liberada: (d) => `desistiu de ${d.animal.nome}` + (d.proximo ? `; a vez passou para ${d.proximo}` : ""),
    vez_expirada: (d) => `o prazo de ${d.de} para ${d.animal.nome} acabou` + (d.proximo ? `; a vez passou para ${d.proximo}` : ""),
    pedido_enviado: (d) => `enviou o pedido para adotar ${d.animal.nome}`,
    pedido_aprovado: (d) => `aprovou a adoção de ${d.animal.nome}`,
    pedido_recusado: (d) => `recusou o pedido para ${d.animal.nome}` + (d.proximo ? `; a vez passou para ${d.proximo}` : ""),
    decisao_conflito: (d) => `foi bloqueado: ${d.motivo}`,
    demo_reiniciada: () => "reiniciou a demonstração",
};

const TOM_EVENTO = {
    vez_concedida: "ok", pedido_aprovado: "ok", pedido_enviado: "ok",
    vez_negada: "erro", decisao_conflito: "erro",
    vez_expirada: "alerta", fila_entrou: "alerta", pedido_recusado: "alerta",
};

function adicionarAoRegistro(evento, novo) {
    const descrever = DESCRICAO_EVENTO[evento.evento] || (() => evento.evento);
    let texto;
    try { texto = descrever(evento.dados || {}); } catch { texto = evento.evento; }
    const item = document.createElement("li");
    item.className = "tom-" + (TOM_EVENTO[evento.evento] || "neutro") + (novo ? " chegou" : "");
    item.innerHTML = `<span class="num">#${evento.seq}</span><span class="lam">L${evento.lamport}</span>
        <span><b>${esc(evento.autor)}</b> ${esc(texto)}</span><time>${formatarHora(evento.hora)}</time>`;
    const lista = $("registro");
    lista.prepend(item);
    while (lista.children.length > 80) lista.lastChild.remove();
}

// ---------------------------------------------------------------------------
// Barra "sua vez"
// ---------------------------------------------------------------------------

function renderizarBarraVez() {
    const barra = $("barraVez");
    const animais = [...replica.animais.values()];
    const comVez = animais.find((a) => situacao(a).minhaVez);
    const naFila = animais.find((a) => situacao(a).posicao);

    if (comVez) {
        barra.innerHTML = `
            <span class="contador" data-prazo-texto="${comVez.reservado_ate}">${segundosRestantes(comVez)}</span>
            <div class="texto"><b>Sua vez com ${esc(comVez.nome)}</b><span>Envie o pedido antes do tempo acabar</span></div>
            <button class="botao botao-primario" data-acao="pedir" data-id="${comVez.id}">Enviar</button>`;
        barra.hidden = false;
    } else if (naFila) {
        const { posicao } = situacao(naFila);
        barra.innerHTML = `
            <span class="contador espera">${posicao}º</span>
            <div class="texto"><b>Na fila de ${esc(naFila.nome)}</b><span>Avisamos quando chegar a sua vez</span></div>
            <a class="botao botao-texto" href="#animal/${naFila.id}">Ver</a>`;
        barra.hidden = false;
    } else {
        barra.hidden = true;
    }
}

// ---------------------------------------------------------------------------
// Renderização geral e contagem regressiva
// ---------------------------------------------------------------------------

function renderizar() {
    const rota = rotaAtual();
    if (rota.tela === "inicio") renderizarInicio();
    if (rota.tela === "animal") renderizarDetalhe(rota.id);
    if (rota.tela === "aovivo") renderizarAoVivo();
    renderizarPedidos(); // também atualiza o selo da navegação
    renderizarBarraVez();
    atualizarMetricas();
    verificarDialogoPedido();
}

// A cada segundo, só os números do prazo mudam (sem redesenhar a tela).
setInterval(() => {
    document.querySelectorAll("[data-prazo-texto]").forEach((el) => {
        const ate = Number(el.dataset.prazoTexto);
        if (ate) el.textContent = Math.max(0, Math.ceil(ate - agoraServidor()));
    });
    document.querySelectorAll("[data-prazo]").forEach((el) => {
        const restante = Math.max(0, Number(el.dataset.prazo) - agoraServidor());
        el.style.width = Math.min(100, (restante / duracaoReserva) * 100) + "%";
    });
    if (pedidoAberto) {
        const animal = replica.animais.get(pedidoAberto);
        if (animal) $("prazoPedido").textContent = segundosRestantes(animal);
    }
}, 1000);

// ---------------------------------------------------------------------------
// Notificações para este aparelho
// ---------------------------------------------------------------------------

function notificar(evento, eraMinhaFila) {
    const d = evento.dados || {};
    const animal = d.animal;
    const nome = animal?.nome || "";
    const meu = evento.cliente === clienteId;

    // A vez chegou para mim (vim da fila)
    if (animal && animal.status === "RESERVADO" && animal.reservado_por === clienteId && evento.evento !== "vez_concedida") {
        alertar(`Chegou a sua vez com ${nome}. Você tem ${duracaoReserva} segundos para enviar o pedido.`);
    }
    if (evento.evento === "vez_expirada" && d.de_cliente === clienteId) {
        alertar(`Seu prazo para ${nome} acabou.`, "erro");
    }
    // Quem decidiu já recebe o aviso da própria ação; não avisa de novo como adotante.
    if (evento.evento === "pedido_aprovado" && d.para === clienteId && !meu) {
        alertar(`Seu pedido foi aprovado! ${nome} vai para casa com você.`, "ok");
    } else if (evento.evento === "pedido_aprovado" && eraMinhaFila) {
        alertar(`${nome} foi adotado por outra pessoa.`, "info");
    }
    if (evento.evento === "pedido_recusado" && d.para === clienteId && !meu) {
        alertar(`Seu pedido para ${nome} não foi aprovado desta vez.`, "erro");
    }

    if (meu) return;
    if (evento.evento === "pedido_enviado") aviso(`${evento.autor} quer adotar ${nome}. Veja em Pedidos.`, "destaque");
    if (evento.evento === "animal_cadastrado") aviso(`Novo animal para adoção: ${nome}.`);
}

// ---------------------------------------------------------------------------
// Ações
// ---------------------------------------------------------------------------

async function reservar(id) {
    pedirPermissaoDeNotificacao(); // os navegadores só permitem pedir num toque
    try {
        const r = await chamar(`/api/animais/${id}/reservar`);
        if (r.posicao === 0) aviso("A vez é sua. Envie o pedido antes do tempo acabar.", "ok");
        else aviso(`Você entrou na fila em ${r.posicao}º lugar.`, "destaque");
    } catch (erro) {
        aviso(erro.message, "erro");
    }
}

function abrirPedido(id) {
    const animal = replica.animais.get(id);
    if (!animal) return;
    pedidoAberto = id;
    $("tituloPedido").textContent = `Adotar ${animal.nome}`;
    $("prazoPedido").textContent = segundosRestantes(animal);
    $("dialogoPedido").showModal();
}

function fecharPedido() {
    pedidoAberto = null;
    if ($("dialogoPedido").open) $("dialogoPedido").close();
}

function verificarDialogoPedido() {
    if (!pedidoAberto) return;
    const animal = replica.animais.get(pedidoAberto);
    if (!animal || !situacao(animal).minhaVez) {
        fecharPedido();
        if (animal && animal.status !== "EM_ANALISE") aviso("O tempo para enviar o pedido acabou.", "erro");
    }
}

$("formPedido").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const id = pedidoAberto;
    const botao = ev.submitter || ev.target.querySelector("[type=submit]");
    botao.disabled = true;
    try {
        const r = await chamar(`/api/animais/${id}/pedido`, {
            telefone: $("telefonePedido").value,
            mensagem: $("mensagemPedido").value,
        });
        pedidoAberto = null;
        $("dialogoPedido").close();
        $("mensagemPedido").value = "";
        memoria.gravar("petamigo.telefone", $("telefonePedido").value);
        aviso(r.mensagem, "ok");
    } catch (erro) {
        aviso(erro.message, "erro");
    } finally {
        botao.disabled = false;
    }
});

$("cancelarPedido").addEventListener("click", fecharPedido);
$("dialogoPedido").addEventListener("close", () => { pedidoAberto = null; });
$("telefonePedido").value = memoria.ler("petamigo.telefone") || "";

document.addEventListener("click", async (ev) => {
    const abrir = ev.target.closest("[data-abrir]");
    if (abrir) { location.hash = `animal/${abrir.dataset.abrir}`; return; }

    const botao = ev.target.closest("button[data-acao]");
    if (!botao) return;
    const id = Number(botao.dataset.id);

    switch (botao.dataset.acao) {
        case "reservar":
            botao.disabled = true;
            await reservar(id);
            botao.disabled = false;
            break;
        case "pedir":
            abrirPedido(id);
            break;
        case "desistir":
            try { await chamar(`/api/animais/${id}/desistir`); } catch (erro) { aviso(erro.message, "erro"); }
            break;
        case "aprovar":
        case "recusar":
            botao.disabled = true;
            try {
                const r = await chamar(`/api/pedidos/${id}/${botao.dataset.acao}`, { versao: Number(botao.dataset.versao) });
                aviso(r.mensagem, "ok");
            } catch (erro) {
                aviso(erro.message, "erro", 5000);
            }
            botao.disabled = false;
            break;
    }
});

// Busca e categorias
$("busca").addEventListener("input", (ev) => { textoBusca = ev.target.value; renderizarInicio(); });

$("categorias").addEventListener("click", (ev) => {
    const botao = ev.target.closest(".categoria");
    if (!botao) return;
    filtroEspecie = botao.dataset.especie;
    document.querySelectorAll(".categoria").forEach((c) => {
        c.classList.toggle("ativa", c === botao);
        c.setAttribute("aria-selected", c === botao);
    });
    renderizarInicio();
});

// Cadastro
$("formAnimal").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const form = ev.target;
    const erro = $("erroCadastro");
    erro.textContent = "";

    const obrigatorios = ["nomeAnimal", "idade", "descricao", "responsavel"];
    const vazio = obrigatorios.find((campo) => !$(campo).value.trim());
    if (vazio) {
        erro.textContent = "Preencha nome, idade, descrição e responsável.";
        $(vazio).focus();
        return;
    }

    const valor = (nome) => form.querySelector(`input[name="${nome}"]:checked`).value;
    const botao = form.querySelector("[type=submit]");
    botao.disabled = true;
    try {
        const r = await chamar("/api/animais", {
            nome_animal: $("nomeAnimal").value, especie: valor("especie"), sexo: valor("sexo"),
            porte: valor("porte"), idade: $("idade").value, descricao: $("descricao").value,
            foto: $("foto").value, responsavel: $("responsavel").value, whatsapp: $("whatsapp").value,
        });
        memoria.gravar("petamigo.responsavel", $("responsavel").value);
        memoria.gravar("petamigo.whatsapp", $("whatsapp").value);
        form.reset();
        $("responsavel").value = memoria.ler("petamigo.responsavel") || "";
        $("whatsapp").value = memoria.ler("petamigo.whatsapp") || "";
        aviso("Animal cadastrado. Ele já aparece para todos.", "ok");
        location.hash = `animal/${r.id}`;
    } catch (e) {
        erro.textContent = e.message;
    } finally {
        botao.disabled = false;
    }
});
$("responsavel").value = memoria.ler("petamigo.responsavel") || "";
$("whatsapp").value = memoria.ler("petamigo.whatsapp") || "";

// Ao vivo
$("botaoCorrida").addEventListener("click", async () => {
    const id = $("animalCorrida").value;
    const nome = replica.animais.get(Number(id))?.nome || "o animal";
    const saida = $("resultadoCorrida");
    const botao = $("botaoCorrida");
    botao.disabled = true;
    saida.textContent = "Enviando 10 pedidos ao mesmo tempo...";
    try {
        const resposta = await fetch(`/api/demo/corrida/${id}?quantidade=10`, { method: "POST" });
        const r = await resposta.json();
        if (r.negados === r.tentativas) {
            saida.textContent = `${r.tentativas} pedidos, todos recusados: ${nome} já foi adotado.`;
        } else {
            const vencedor = r.vencedores.length
                ? `Robô ${r.vencedores[0]} ficou com a vez`
                : `Ninguém pegou a vez (${nome} já estava com outra pessoa)`;
            saida.textContent = `${r.tentativas} pedidos simultâneos. ${vencedor} e ${r.fila.length} entraram na fila em ordem de chegada.`;
        }
    } catch {
        saida.textContent = "Não foi possível simular agora.";
    } finally {
        botao.disabled = false;
    }
});

$("botaoResync").addEventListener("click", () => ressincronizar("pedido manual"));

$("botaoReiniciar").addEventListener("click", async () => {
    if (!confirm("Liberar todos os animais e apagar os pedidos e filas?")) return;
    try { await chamar("/api/demo/reiniciar"); } catch (erro) { aviso(erro.message, "erro"); }
});

// QR code para a plateia entrar
async function montarQrCode() {
    let url = location.origin + "/";
    if (["localhost", "127.0.0.1", "[::1]"].includes(location.hostname)) {
        try {
            const { ip } = await fetch("/api/rede").then((r) => r.json());
            if (ip) url = `${location.protocol}//${ip}${location.port ? ":" + location.port : ""}/`;
        } catch { /* fica com o endereço atual */ }
    }
    $("enderecoAtual").textContent = url;
    if (window.QRCode) new QRCode($("qrcode"), { text: url, width: 240, height: 240, colorDark: "#2b2420" });
}

// ---------------------------------------------------------------------------
// Entrada (nome)
// ---------------------------------------------------------------------------

// O botão "Começar" só fica ativo quando há um nome digitado.
function atualizarBotaoEntrar() {
    document.querySelector(".botao-entrar").disabled = !$("campoNome").value.trim();
}
$("campoNome").addEventListener("input", atualizarBotaoEntrar);

function abrirEntrada(podeVoltar) {
    $("campoNome").value = meuNome;
    atualizarBotaoEntrar();
    $("cancelarNome").hidden = !podeVoltar;
    $("entrada").hidden = false;
}

function atualizarPerfil() {
    $("nomePerfil").textContent = meuNome ? primeiroNome(meuNome) : "Perfil";
}

$("formNome").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const nome = $("campoNome").value.trim().slice(0, 40);
    if (!nome) { $("campoNome").focus(); return; }
    const mudou = meuNome && meuNome !== nome;
    meuNome = nome;
    memoria.gravar("petamigo.nome", meuNome);
    $("entrada").hidden = true;
    atualizarPerfil();
    pedirPermissaoDeNotificacao();
    if (!socket) conectar();
    else if (mudou) socket.close(); // reconecta com o nome novo
    renderizar();
});

$("cancelarNome").addEventListener("click", () => { $("entrada").hidden = true; });
$("botaoPerfil").addEventListener("click", () => abrirEntrada(true));

// ---------------------------------------------------------------------------
// Início
// ---------------------------------------------------------------------------

montarQrCode();
atualizarPerfil();
mostrarTela();
if (meuNome) conectar();
else abrirEntrada(false);
