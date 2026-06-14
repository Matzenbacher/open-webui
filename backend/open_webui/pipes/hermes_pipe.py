"""
title: Hermes Agent
author: Antigravity
description: Canal de comunicação assíncrona com o Hermes Agent via Docker API.
version: 1.0
requirements: httpx
"""

import json
import os
import httpx
from pydantic import BaseModel, Field
from typing import AsyncGenerator

MAPPING_FILE = "/app/backend/data/hermes_sessions.json"

class Pipe:
    class Valves(BaseModel):
        HERMES_API_URL: str = Field(
            default="http://hermes_webui:8787", 
            description="URL interna do container do Hermes WebUI"
        )
        HERMES_PASSWORD: str = Field(
            default="admin1234", 
            description="Senha de acesso ao Hermes WebUI"
        )
        HERMES_WORKSPACE: str = Field(
            default="/workspace", 
            description="Diretório de workspace do Hermes"
        )

    def __init__(self):
        self.valves = self.Valves()
        self.client_cookies = {}

    def pipes(self):
        return [{"id": "hermes_agent", "name": "Hermes"}]

    async def _login(self, client: httpx.AsyncClient):
        """Realiza a autenticação no Hermes WebUI para obter o cookie de sessão."""
        try:
            resp = await client.post(
                f"{self.valves.HERMES_API_URL}/api/auth/login",
                json={"password": self.valves.HERMES_PASSWORD},
                timeout=10.0
            )
            resp.raise_for_status()
            self.client_cookies = dict(resp.cookies)
        except Exception as e:
            raise RuntimeError(f"Falha de autenticação no Hermes: {str(e)}")

    async def _request_with_auth(self, client: httpx.AsyncClient, method: str, url: str, **kwargs) -> httpx.Response:
        """Executa uma requisição HTTP. Faz login automático se retornar 401."""
        if not self.client_cookies:
            await self._login(client)

        client.cookies.update(self.client_cookies)
        resp = await client.request(method, url, **kwargs)

        is_auth_error = resp.status_code == 401
        if not is_auth_error and resp.status_code == 200:
            try:
                content = resp.json()
                if isinstance(content, dict) and "Authentication required" in content.get("error", ""):
                    is_auth_error = True
            except Exception:
                pass

        if is_auth_error:
            await self._login(client)
            client.cookies.update(self.client_cookies)
            resp = await client.request(method, url, **kwargs)

        return resp

    async def pipe(self, body: dict, __chat_id__: str = None) -> AsyncGenerator[str, None]:
        messages = body.get("messages", [])
        if not messages:
            yield "Nenhuma mensagem encontrada."
            return

        last_msg = messages[-1].get("content", "")

        if not __chat_id__:
            __chat_id__ = "default_session"

        hermes_session_id = self._get_hermes_session_id(__chat_id__)

        async with httpx.AsyncClient(timeout=300.0) as client:
            # Se não existe sessão no Hermes, cria uma nova
            if not hermes_session_id:
                try:
                    resp = await self._request_with_auth(
                        client, "POST",
                        f"{self.valves.HERMES_API_URL}/api/session/new",
                        json={"workspace": self.valves.HERMES_WORKSPACE}
                    )
                    resp.raise_for_status()
                    session_data = resp.json()
                    hermes_session_id = session_data["session"]["session_id"]
                    self._save_hermes_session_id(__chat_id__, hermes_session_id)
                except Exception as e:
                    yield f"Erro ao criar sessão no Hermes: {str(e)}"
                    return

            # Inicia o chat no Hermes
            try:
                chat_resp = await self._request_with_auth(
                    client, "POST",
                    f"{self.valves.HERMES_API_URL}/api/chat/start",
                    json={
                        "session_id": hermes_session_id,
                        "message": last_msg
                    }
                )

                # Se der 404, significa que a sessão expirou/sumiu do container. Recria.
                if chat_resp.status_code == 404:
                    resp = await self._request_with_auth(
                        client, "POST",
                        f"{self.valves.HERMES_API_URL}/api/session/new",
                        json={"workspace": self.valves.HERMES_WORKSPACE}
                    )
                    resp.raise_for_status()
                    session_data = resp.json()
                    hermes_session_id = session_data["session"]["session_id"]
                    self._save_hermes_session_id(__chat_id__, hermes_session_id)

                    chat_resp = await self._request_with_auth(
                        client, "POST",
                        f"{self.valves.HERMES_API_URL}/api/chat/start",
                        json={
                            "session_id": hermes_session_id,
                            "message": last_msg
                        }
                    )

                chat_resp.raise_for_status()
                chat_data = chat_resp.json()
                stream_id = chat_data["stream_id"]
            except Exception as e:
                yield f"Erro ao iniciar chat no Hermes: {str(e)}"
                return

            # Consome o streaming SSE
            try:
                client.cookies.update(self.client_cookies)
                async with client.stream(
                    "GET",
                    f"{self.valves.HERMES_API_URL}/api/chat/stream",
                    params={"stream_id": stream_id}
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if not data_str:
                                continue
                            try:
                                event_data = json.loads(data_str)
                                if isinstance(event_data, dict):
                                    text = event_data.get("text", "")
                                    if text:
                                        yield text
                            except json.JSONDecodeError:
                                pass
            except Exception as e:
                yield f"\n[Erro na transmissão de streaming do Hermes: {str(e)}]"

    def _get_hermes_session_id(self, chat_id: str) -> str:
        if not os.path.exists(MAPPING_FILE):
            return None
        try:
            with open(MAPPING_FILE, "r") as f:
                data = json.load(f)
            return data.get(chat_id)
        except Exception:
            return None

    def _save_hermes_session_id(self, chat_id: str, hermes_id: str):
        data = {}
        if os.path.exists(MAPPING_FILE):
            try:
                with open(MAPPING_FILE, "r") as f:
                    data = json.load(f)
            except Exception:
                pass
        data[chat_id] = hermes_id
        try:
            with open(MAPPING_FILE, "w") as f:
                json.dump(data, f)
        except Exception:
            pass
