"""Conexões WebSocket abertas (uma por aparelho) e envio de mensagens para todas."""

import asyncio
import json

from fastapi import WebSocket


class Conexoes:
    def __init__(self):
        self._ativas: dict[WebSocket, dict] = {}
        # Um envio por vez: os eventos saem para todos na mesma ordem.
        self._trava = asyncio.Lock()

    async def entrar(self, ws: WebSocket, cliente_id: str, nome: str):
        await ws.accept()
        self._ativas[ws] = {"cliente_id": cliente_id, "nome": nome}
        await self.transmitir([self.presenca()])

    async def sair(self, ws: WebSocket):
        if self._ativas.pop(ws, None) is not None:
            await self.transmitir([self.presenca()])

    def presenca(self):
        aparelhos = {c["cliente_id"]: c["nome"] for c in self._ativas.values()}
        return {"tipo": "presenca", "online": len(aparelhos),
                "nomes": sorted(set(aparelhos.values()), key=str.lower)}

    async def transmitir(self, mensagens):
        if not mensagens:
            return
        caidas = []
        async with self._trava:
            for mensagem in mensagens:
                texto = json.dumps(mensagem, ensure_ascii=False)
                destinos = list(self._ativas)
                resultados = await asyncio.gather(*(self._enviar(ws, texto) for ws in destinos))
                caidas += [ws for ws, ok in zip(destinos, resultados) if not ok]
        if caidas:
            for ws in caidas:
                self._ativas.pop(ws, None)
            await self.transmitir([self.presenca()])

    @staticmethod
    async def _enviar(ws: WebSocket, texto: str) -> bool:
        try:
            await asyncio.wait_for(ws.send_text(texto), timeout=5)
            return True
        except Exception:
            return False


conexoes = Conexoes()
