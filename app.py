"""Servidor HTTP e WebSocket do PetAmigo.

Rodar localmente:  python app.py
"""

import asyncio
import os
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import regras
from banco import criar_banco
from tempo_real import conexoes

PASTA = Path(__file__).parent


async def executar(operacao):
    """Roda a operação numa thread (requisições concorrentes de verdade) e,
    depois do COMMIT, envia os eventos gerados para todos os aparelhos."""
    try:
        resultado, eventos = await asyncio.to_thread(regras.transacao, operacao)
    except regras.Conflito as conflito:
        await conexoes.transmitir(conflito.eventos)
        raise HTTPException(409, conflito.mensagem) from None
    except regras.NaoEncontrado as erro:
        raise HTTPException(404, str(erro)) from None
    await conexoes.transmitir(eventos)
    return resultado


async def vigiar_prazos():
    while True:
        await asyncio.sleep(1)
        try:
            await executar(regras.expirar_reservas)
        except Exception as erro:
            print("Falha ao verificar prazos:", erro)


@asynccontextmanager
async def ciclo_de_vida(_app):
    criar_banco()
    regras.relogio.iniciar(regras.maior_lamport())
    tarefa = asyncio.create_task(vigiar_prazos())
    yield
    tarefa.cancel()


app = FastAPI(title="PetAmigo", lifespan=ciclo_de_vida)
app.mount("/static", StaticFiles(directory=PASTA / "static"), name="static")


@app.exception_handler(RequestValidationError)
async def dados_invalidos(_request: Request, _erro: RequestValidationError):
    return JSONResponse({"detail": "Confira os campos preenchidos."}, status_code=422)


# ---------------------------------------------------------------------------
# Corpo das requisições
# ---------------------------------------------------------------------------

class Identificacao(BaseModel):
    cliente_id: str = Field(min_length=1, max_length=64)
    nome: str = Field(min_length=1, max_length=40)
    lamport: int = Field(default=0, ge=0)
    papel: Literal["adotante", "doador"] | None = None

    def exigir(self, papel):
        """O papel é escolhido na entrada. Cada ação só vale para um dos papéis."""
        if self.papel != papel:
            mensagem = {
                "adotante": "Só quem entrou como adotante pode fazer isso.",
                "doador": "Só quem entrou como doador pode fazer isso.",
            }[papel]
            raise HTTPException(403, mensagem)
        return self

    def autor(self):
        return regras.Autor(self.cliente_id, self.nome.strip() or "Anônimo", self.lamport)


class NovoAnimal(Identificacao):
    nome_animal: str = Field(min_length=1, max_length=60)
    especie: str = Field(min_length=1, max_length=30)
    idade: str = Field(min_length=1, max_length=30)
    porte: str = Field(min_length=1, max_length=30)
    sexo: str = Field(min_length=1, max_length=30)
    descricao: str = Field(min_length=1, max_length=600)
    foto: str = Field(default="", max_length=500)
    responsavel: str = Field(min_length=1, max_length=80)
    whatsapp: str = Field(default="", max_length=25)


class NovoPedido(Identificacao):
    telefone: str = Field(default="", max_length=30)
    mensagem: str = Field(default="", max_length=500)


class Decisao(Identificacao):
    versao: int


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@app.get("/")
async def pagina_inicial():
    return FileResponse(PASTA / "templates" / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/api/saude")
async def saude():
    return {"ok": True}


@app.get("/api/estado")
async def estado():
    return await asyncio.to_thread(regras.ler_estado)


@app.get("/api/rede")
async def endereco_na_rede():
    """IP do computador no Wi-Fi, para o QR code não apontar para 'localhost'."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))  # não envia nada, só descobre a interface de saída
        return {"ip": s.getsockname()[0]}
    except OSError:
        return {"ip": None}
    finally:
        s.close()


@app.post("/api/animais")
async def cadastrar_animal(corpo: NovoAnimal):
    corpo.exigir("doador")
    dados = {
        "nome": corpo.nome_animal.strip(), "especie": corpo.especie, "idade": corpo.idade.strip(),
        "porte": corpo.porte, "sexo": corpo.sexo, "descricao": corpo.descricao.strip(),
        "foto": corpo.foto.strip() or None, "responsavel": corpo.responsavel.strip(),
        "whatsapp": corpo.whatsapp.strip() or None,
    }
    return await executar(lambda conn: regras.cadastrar_animal(conn, dados, corpo.autor()))


@app.post("/api/animais/{animal_id}/reservar")
async def reservar(animal_id: int, corpo: Identificacao):
    corpo.exigir("adotante")
    return await executar(lambda conn: regras.querer_adotar(conn, animal_id, corpo.autor()))


@app.post("/api/animais/{animal_id}/desistir")
async def desistir(animal_id: int, corpo: Identificacao):
    corpo.exigir("adotante")
    return await executar(lambda conn: regras.desistir(conn, animal_id, corpo.autor()))


@app.post("/api/animais/{animal_id}/pedido")
async def enviar_pedido(animal_id: int, corpo: NovoPedido):
    corpo.exigir("adotante")
    return await executar(lambda conn: regras.enviar_pedido(
        conn, animal_id, corpo.autor(), corpo.telefone.strip(), corpo.mensagem.strip()))


@app.post("/api/pedidos/{pedido_id}/aprovar")
async def aprovar(pedido_id: int, corpo: Decisao):
    corpo.exigir("doador")
    return await executar(lambda conn: regras.decidir(conn, pedido_id, True, corpo.versao, corpo.autor()))


@app.post("/api/pedidos/{pedido_id}/recusar")
async def recusar(pedido_id: int, corpo: Decisao):
    corpo.exigir("doador")
    return await executar(lambda conn: regras.decidir(conn, pedido_id, False, corpo.versao, corpo.autor()))


@app.post("/api/demo/corrida/{animal_id}")
async def simular_corrida(animal_id: int, quantidade: int = 10):
    """Dispara N pedidos de vez ao mesmo tempo. Um fica com a vez e o resto vai para a fila."""
    quantidade = max(2, min(quantidade, 30))

    async def robo(i):
        autor = regras.Autor(f"robo-{i}", f"Robô {i}")
        try:
            r = await executar(lambda conn: regras.querer_adotar(conn, animal_id, autor))
            return i, r["posicao"]
        except HTTPException:
            return i, None

    resultados = await asyncio.gather(*(robo(i) for i in range(1, quantidade + 1)))
    na_fila = sorted((r for r in resultados if r[1]), key=lambda r: r[1])
    return {"tentativas": quantidade,
            "vencedores": [i for i, pos in resultados if pos == 0],
            "fila": [i for i, _ in na_fila],
            "negados": sum(1 for _, pos in resultados if pos is None)}


@app.post("/api/demo/reiniciar")
async def reiniciar(corpo: Identificacao):
    return await executar(lambda conn: regras.reiniciar(conn, corpo.autor()))


@app.websocket("/ws")
async def canal_tempo_real(ws: WebSocket, cliente: str = "anonimo", nome: str = "Anônimo"):
    await conexoes.entrar(ws, cliente[:64], nome[:40])
    try:
        while True:
            await ws.receive_text()  # o aparelho manda "ping" para manter a conexão viva
    except WebSocketDisconnect:
        pass
    finally:
        await conexoes.sair(ws)


if __name__ == "__main__":
    import uvicorn

    porta = int(os.environ.get("PORT", 8000))
    ip = asyncio.run(endereco_na_rede())["ip"]
    print(f"\nPetAmigo rodando\n  Neste computador: http://localhost:{porta}")
    if ip:
        print(f"  No celular (mesmo Wi-Fi): http://{ip}:{porta}")
    print()
    uvicorn.run("app:app", host="0.0.0.0", port=porta)
