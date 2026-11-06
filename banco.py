"""Conexão com o SQLite, criação das tabelas e dados iniciais."""

import os
import sqlite3
from pathlib import Path

CAMINHO_BANCO = os.environ.get("PETAMIGO_DB", str(Path(__file__).with_name("petamigo.db")))

# Quantos segundos a pessoa tem para enviar o pedido depois de pegar a vez.
DURACAO_RESERVA = int(os.environ.get("DURACAO_RESERVA", "60"))


def conectar() -> sqlite3.Connection:
    # isolation_level=None: as transações são abertas à mão com BEGIN IMMEDIATE
    # (ver regras.transacao), que é o que garante a exclusão mútua.
    conn = sqlite3.connect(CAMINHO_BANCO, timeout=20, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 20000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


ESQUEMA = """
CREATE TABLE IF NOT EXISTS animais (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    nome            TEXT NOT NULL,
    especie         TEXT NOT NULL,
    idade           TEXT NOT NULL,
    porte           TEXT NOT NULL,
    sexo            TEXT NOT NULL,
    descricao       TEXT NOT NULL,
    foto            TEXT,
    responsavel     TEXT NOT NULL,
    whatsapp        TEXT,
    status          TEXT NOT NULL DEFAULT 'DISPONIVEL'
                    CHECK (status IN ('DISPONIVEL', 'RESERVADO', 'EM_ANALISE', 'ADOTADO')),
    reservado_por   TEXT,   -- id do aparelho que está com a vez
    reservado_nome  TEXT,
    reservado_ate   REAL,   -- prazo da vez (relógio do servidor)
    adotado_por     TEXT,
    adotado_cliente TEXT,
    versao          INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS pedidos (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id    INTEGER NOT NULL REFERENCES animais(id),
    cliente_id   TEXT NOT NULL,
    adotante     TEXT NOT NULL,
    telefone     TEXT,
    mensagem     TEXT,
    status       TEXT NOT NULL DEFAULT 'PENDENTE'
                 CHECK (status IN ('PENDENTE', 'APROVADO', 'RECUSADO')),
    decidido_por TEXT,
    versao       INTEGER NOT NULL DEFAULT 1,
    criado_em    REAL NOT NULL
);

-- Fila de espera por animal. A ordem de chegada é dada pelo id.
CREATE TABLE IF NOT EXISTS fila (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id  INTEGER NOT NULL REFERENCES animais(id),
    cliente_id TEXT NOT NULL,
    nome       TEXT NOT NULL,
    entrou_em  REAL NOT NULL,
    UNIQUE (animal_id, cliente_id)
);

-- Registro de eventos. Cada alteração grava um evento na mesma transação;
-- o seq define a ordem global que os aparelhos seguem.
CREATE TABLE IF NOT EXISTS eventos (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    lamport    INTEGER NOT NULL,
    tipo       TEXT NOT NULL,
    autor      TEXT,
    cliente_id TEXT,
    dados      TEXT NOT NULL,
    hora       REAL NOT NULL
);
"""

ANIMAIS_INICIAIS = [
    ("Bob", "Cachorro", "2 anos", "Médio", "Macho",
     "Dócil e brincalhão, adora passear e se dá bem com crianças. Vacinado e castrado.",
     "https://images.unsplash.com/photo-1552053831-71594a27632d?w=600",
     "ONG Amor Animal"),
    ("Luna", "Gato", "1 ano", "Pequeno", "Fêmea",
     "Calma, gosta de colo e de ficar na janela. Já usa caixa de areia.",
     "https://images.unsplash.com/photo-1573865526739-10659fec78a5?w=600",
     "ONG Amor Animal"),
    ("Thor", "Cachorro", "3 anos", "Grande", "Macho",
     "Leal e esperto, aprende comandos rápido. Precisa de espaço para correr.",
     "https://images.unsplash.com/photo-1543466835-00a7907e9de1?w=600",
     "Protetora Ana Lima"),
    ("Mel", "Gato", "6 meses", "Pequeno", "Fêmea",
     "Filhote curiosa, brinca o dia inteiro. Tomou a primeira dose da vacina.",
     "https://images.unsplash.com/photo-1514888286974-6c03e2ca1dba?w=600",
     "Protetora Ana Lima"),
]


def criar_banco() -> None:
    conn = conectar()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(ESQUEMA)
        if conn.execute("SELECT COUNT(*) FROM animais").fetchone()[0] == 0:
            conn.executemany("""
                INSERT INTO animais (nome, especie, idade, porte, sexo, descricao, foto, responsavel)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, ANIMAIS_INICIAIS)
    finally:
        conn.close()
