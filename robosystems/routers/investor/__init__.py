"""Investor API routers."""

from fastapi import APIRouter

from .holdings import router as holdings_router
from .portfolios import router as portfolios_router
from .positions import router as positions_router
from .securities import router as securities_router

router = APIRouter()
router.include_router(portfolios_router)
router.include_router(securities_router)
router.include_router(positions_router)
router.include_router(holdings_router)
