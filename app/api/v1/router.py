"""Agregador dos routers da versao 1."""

from fastapi import APIRouter

from app.api.v1 import events, health

TAG_EVENTS = "events"
TAG_HEALTH = "health"

api_router = APIRouter()
api_router.include_router(events.router, tags=[TAG_EVENTS])
api_router.include_router(health.router, tags=[TAG_HEALTH])
