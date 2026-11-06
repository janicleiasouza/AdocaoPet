"""Regras de adoção e os mecanismos distribuídos do PetAmigo.

Exclusão mútua
    Toda alteração roda numa transação BEGIN IMMEDIATE, que pega a trava de
    escrita do SQLite. Se vinte aparelhos pedirem o mesmo animal ao mesmo
    tempo, as transações acontecem uma depois da outra: a primeira pega a vez
    e as outras já enxergam o animal reservado e entram na fila.
    A vez é um prazo (lease) de DURACAO_RESERVA segundos que expira sozinho,
    para que um aparelho que sumiu não trave o animal para sempre.
    A decisão sobre um pedido usa controle otimista de versão.

Comunicação
    Cada alteração gera um evento que o servidor envia a todos os aparelhos
    pelo WebSocket. O evento leva o carimbo do relógio lógico de Lamport.

Consistência
    O banco é a fonte da verdade. O evento é gravado na mesma transação da
    alteração (padrão outbox) e recebe um número de sequência (seq). O
    aparelho aplica os eventos na ordem do seq e baixa o estado completo
    quando percebe que perdeu algum.

Cada operação recebe a conexão aberta e devolve (resultado, eventos).
"""

import json
import threading
import time
from dataclasses import dataclass

from banco import DURACAO_RESERVA, conectar


@dataclass(frozen=True)
class Autor:
    cliente_id: str
    nome: str
    lamport: int = 0


SERVIDOR = Autor("servidor", "Servidor")


class NaoEncontrado(Exception):
    pass


class Conflito(Exception):
    """Ação recusada por concorrência. Os eventos dela são gravados e enviados mesmo assim."""

    def __init__(self, mensagem, eventos=()):
        super().__init__(mensagem)
        self.mensagem = mensagem
        self.eventos = list(eventos)


# ---------------------------------------------------------------------------
# Relógio lógico de Lamport
# ---------------------------------------------------------------------------

class RelogioLamport:
    def __init__(self):
        self.valor = 0
        self._trava = threading.Lock()

    def iniciar(self, valor):
        with self._trava:
            self.valor = max(self.valor, int(valor or 0))

    def marcar(self, recebido=0):
        """Ao receber uma mensagem: max(local, recebido) + 1."""
        with self._trava:
            self.valor = max(self.valor, int(recebido or 0)) + 1
            return self.valor


relogio = RelogioLamport()


# ---------------------------------------------------------------------------
# Transação e eventos
# ---------------------------------------------------------------------------

def transacao(operacao):
    """Executa operacao(conn) com a trava de escrita do banco (seção crítica)."""
    conn = conectar()
    try:
        conn.execute("BEGIN IMMEDIATE")
        resultado = operacao(conn)
        conn.execute("COMMIT")
        return resultado
    except Conflito:
        conn.execute("COMMIT")  # o evento do conflito fica no registro
        raise
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _mensagem(seq, lamport, tipo, autor, cliente_id, dados, hora):
    return {"tipo": "evento", "seq": seq, "lamport": lamport, "evento": tipo,
            "autor": autor, "cliente": cliente_id, "dados": dados, "hora": hora}


def registrar_evento(conn, tipo, autor: Autor, dados):
    lamport = relogio.marcar(autor.lamport)
    hora = time.time()
    cur = conn.execute(
        "INSERT INTO eventos (lamport, tipo, autor, cliente_id, dados, hora) VALUES (?, ?, ?, ?, ?, ?)",
        (lamport, tipo, autor.nome, autor.cliente_id, json.dumps(dados, ensure_ascii=False), hora),
    )
    return _mensagem(cur.lastrowid, lamport, tipo, autor.nome, autor.cliente_id, dados, hora)


def evento_do_animal(conn, tipo, autor, animal_id, **extra):
    """Evento que leva o estado atual do animal e da fila dele."""
    dados = {"animal": buscar_animal(conn, animal_id), "fila": fila_do_animal(conn, animal_id), **extra}
    return registrar_evento(conn, tipo, autor, dados)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def buscar_animal(conn, animal_id):
    linha = conn.execute("SELECT * FROM animais WHERE id = ?", (animal_id,)).fetchone()
    if linha is None:
        raise NaoEncontrado("Animal não encontrado.")
    return dict(linha)


def buscar_pedido(conn, pedido_id):
    linha = conn.execute("""
        SELECT p.*, a.nome AS animal_nome FROM pedidos p
        JOIN animais a ON a.id = p.animal_id WHERE p.id = ?
    """, (pedido_id,)).fetchone()
    if linha is None:
        raise NaoEncontrado("Pedido não encontrado.")
    return dict(linha)


def fila_do_animal(conn, animal_id):
    return [dict(l) for l in conn.execute(
        "SELECT cliente_id, nome FROM fila WHERE animal_id = ? ORDER BY id", (animal_id,))]


def maior_lamport():
    conn = conectar()
    try:
        return conn.execute("SELECT COALESCE(MAX(lamport), 0) FROM eventos").fetchone()[0]
    finally:
        conn.close()


def ler_estado():
    """Retrato completo do sistema, lido numa única transação (visão consistente)."""
    conn = conectar()
    try:
        conn.execute("BEGIN")
        animais = [dict(l) for l in conn.execute("SELECT * FROM animais ORDER BY id")]
        pedidos = [dict(l) for l in conn.execute("""
            SELECT p.*, a.nome AS animal_nome FROM pedidos p
            JOIN animais a ON a.id = p.animal_id ORDER BY p.id DESC
        """)]
        fila = [dict(l) for l in conn.execute(
            "SELECT animal_id, cliente_id, nome FROM fila ORDER BY id")]
        seq = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM eventos").fetchone()[0]
        eventos = [
            _mensagem(l["seq"], l["lamport"], l["tipo"], l["autor"], l["cliente_id"],
                      json.loads(l["dados"]), l["hora"])
            for l in conn.execute("SELECT * FROM eventos ORDER BY seq DESC LIMIT 50")
        ]
        conn.execute("COMMIT")
    finally:
        conn.close()
    return {"animais": animais, "pedidos": pedidos, "fila": fila, "seq": seq,
            "eventos": eventos, "lamport": relogio.valor, "agora": time.time(),
            "duracao_reserva": DURACAO_RESERVA}


# ---------------------------------------------------------------------------
# Vez e fila
# ---------------------------------------------------------------------------

def _dar_vez(conn, animal_id, cliente_id, nome):
    conn.execute("""
        UPDATE animais SET status = 'RESERVADO', reservado_por = ?, reservado_nome = ?,
               reservado_ate = ?, versao = versao + 1
         WHERE id = ?
    """, (cliente_id, nome, time.time() + DURACAO_RESERVA, animal_id))


def _passar_vez(conn, animal_id):
    """Entrega a vez ao primeiro da fila ou deixa o animal disponível.
    Devolve o nome de quem recebeu a vez (ou None)."""
    proximo = conn.execute(
        "SELECT * FROM fila WHERE animal_id = ? ORDER BY id LIMIT 1", (animal_id,)).fetchone()
    if proximo is None:
        conn.execute("""
            UPDATE animais SET status = 'DISPONIVEL', reservado_por = NULL,
                   reservado_nome = NULL, reservado_ate = NULL, versao = versao + 1
             WHERE id = ?
        """, (animal_id,))
        return None
    conn.execute("DELETE FROM fila WHERE id = ?", (proximo["id"],))
    _dar_vez(conn, animal_id, proximo["cliente_id"], proximo["nome"])
    return proximo["nome"]


def querer_adotar(conn, animal_id, autor: Autor):
    """Se o animal está livre, a pessoa pega a vez. Senão, entra no fim da fila."""
    animal = buscar_animal(conn, animal_id)

    if animal["status"] == "ADOTADO":
        motivo = f"{animal['nome']} já foi adotado."
        raise Conflito(motivo, [evento_do_animal(conn, "vez_negada", autor, animal_id, motivo=motivo)])

    if animal["reservado_por"] == autor.cliente_id:
        return {"posicao": 0}, []

    if animal["status"] == "DISPONIVEL":
        _dar_vez(conn, animal_id, autor.cliente_id, autor.nome)
        evento = evento_do_animal(conn, "vez_concedida", autor, animal_id)
        return {"posicao": 0, "lamport": evento["lamport"]}, [evento]

    cur = conn.execute("""
        INSERT OR IGNORE INTO fila (animal_id, cliente_id, nome, entrou_em) VALUES (?, ?, ?, ?)
    """, (animal_id, autor.cliente_id, autor.nome, time.time()))
    fila = fila_do_animal(conn, animal_id)
    posicao = next(i for i, p in enumerate(fila, 1) if p["cliente_id"] == autor.cliente_id)
    if cur.rowcount == 0:  # já estava na fila
        return {"posicao": posicao}, []
    evento = evento_do_animal(conn, "fila_entrou", autor, animal_id, posicao=posicao)
    return {"posicao": posicao, "lamport": evento["lamport"]}, [evento]


def desistir(conn, animal_id, autor: Autor):
    """Quem está com a vez passa para o próximo; quem está na fila sai dela."""
    animal = buscar_animal(conn, animal_id)
    if animal["status"] == "RESERVADO" and animal["reservado_por"] == autor.cliente_id:
        proximo = _passar_vez(conn, animal_id)
        evento = evento_do_animal(conn, "vez_liberada", autor, animal_id, proximo=proximo)
        return {"lamport": evento["lamport"]}, [evento]

    cur = conn.execute("DELETE FROM fila WHERE animal_id = ? AND cliente_id = ?",
                       (animal_id, autor.cliente_id))
    if cur.rowcount == 0:
        return {}, []
    evento = evento_do_animal(conn, "fila_saiu", autor, animal_id)
    return {"lamport": evento["lamport"]}, [evento]


def expirar_reservas(conn):
    vencidas = conn.execute("""
        SELECT id, reservado_nome, reservado_por FROM animais
         WHERE status = 'RESERVADO' AND reservado_ate < ?
    """, (time.time(),)).fetchall()
    eventos = []
    for v in vencidas:
        proximo = _passar_vez(conn, v["id"])
        eventos.append(evento_do_animal(conn, "vez_expirada", SERVIDOR, v["id"],
                                        de=v["reservado_nome"], de_cliente=v["reservado_por"],
                                        proximo=proximo))
    return None, eventos


# ---------------------------------------------------------------------------
# Pedidos
# ---------------------------------------------------------------------------

def enviar_pedido(conn, animal_id, autor: Autor, telefone, mensagem):
    """Só quem está com a vez, dentro do prazo, consegue enviar o pedido."""
    animal = buscar_animal(conn, animal_id)
    if not (animal["status"] == "RESERVADO" and animal["reservado_por"] == autor.cliente_id
            and animal["reservado_ate"] >= time.time()):
        raise Conflito("Seu prazo acabou ou a vez está com outra pessoa.")

    cur = conn.execute("""
        INSERT INTO pedidos (animal_id, cliente_id, adotante, telefone, mensagem, criado_em)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (animal_id, autor.cliente_id, autor.nome, telefone or None, mensagem or None, time.time()))
    conn.execute("""
        UPDATE animais SET status = 'EM_ANALISE', reservado_ate = NULL, versao = versao + 1
         WHERE id = ?
    """, (animal_id,))
    evento = evento_do_animal(conn, "pedido_enviado", autor, animal_id,
                              pedido=buscar_pedido(conn, cur.lastrowid))
    return {"mensagem": "Pedido enviado. O responsável já foi avisado.",
            "lamport": evento["lamport"]}, [evento]


def decidir(conn, pedido_id, aprovar: bool, versao: int, autor: Autor):
    """Controle otimista: a decisão só vale se o pedido ainda estiver na versão
    que a pessoa viu. Se dois aparelhos decidirem juntos, só o primeiro vale."""
    novo = "APROVADO" if aprovar else "RECUSADO"
    cur = conn.execute("""
        UPDATE pedidos SET status = ?, decidido_por = ?, versao = versao + 1
         WHERE id = ? AND status = 'PENDENTE' AND versao = ?
    """, (novo, autor.nome, pedido_id, versao))
    pedido = buscar_pedido(conn, pedido_id)

    if cur.rowcount == 0:
        if pedido["status"] != "PENDENTE":
            motivo = (f"Este pedido já foi {pedido['status'].lower()} por "
                      f"{pedido['decidido_por']} em outro aparelho.")
        else:
            motivo = "O pedido mudou em outro aparelho. Tente de novo."
        evento = registrar_evento(conn, "decisao_conflito", autor, {"pedido": pedido, "motivo": motivo})
        raise Conflito(motivo, [evento])

    proximo = None
    if aprovar:
        conn.execute("""
            UPDATE animais SET status = 'ADOTADO', adotado_por = ?, adotado_cliente = ?,
                   reservado_por = NULL, reservado_nome = NULL, reservado_ate = NULL,
                   versao = versao + 1
             WHERE id = ?
        """, (pedido["adotante"], pedido["cliente_id"], pedido["animal_id"]))
        conn.execute("DELETE FROM fila WHERE animal_id = ?", (pedido["animal_id"],))
    else:
        proximo = _passar_vez(conn, pedido["animal_id"])

    evento = evento_do_animal(conn, "pedido_aprovado" if aprovar else "pedido_recusado", autor,
                              pedido["animal_id"], pedido=pedido, para=pedido["cliente_id"],
                              proximo=proximo)
    return {"mensagem": "Adoção aprovada." if aprovar else "Pedido recusado.",
            "lamport": evento["lamport"]}, [evento]


# ---------------------------------------------------------------------------
# Cadastro e demonstração
# ---------------------------------------------------------------------------

def cadastrar_animal(conn, dados: dict, autor: Autor):
    cur = conn.execute("""
        INSERT INTO animais (nome, especie, idade, porte, sexo, descricao, foto, responsavel, whatsapp)
        VALUES (:nome, :especie, :idade, :porte, :sexo, :descricao, :foto, :responsavel, :whatsapp)
    """, dados)
    evento = evento_do_animal(conn, "animal_cadastrado", autor, cur.lastrowid)
    return {"id": cur.lastrowid, "lamport": evento["lamport"]}, [evento]


def reiniciar(conn, autor: Autor):
    conn.execute("DELETE FROM pedidos")
    conn.execute("DELETE FROM fila")
    conn.execute("""
        UPDATE animais SET status = 'DISPONIVEL', reservado_por = NULL, reservado_nome = NULL,
               reservado_ate = NULL, adotado_por = NULL, adotado_cliente = NULL,
               versao = versao + 1
    """)
    evento = registrar_evento(conn, "demo_reiniciada", autor, {})
    return {"lamport": evento["lamport"]}, [evento]
