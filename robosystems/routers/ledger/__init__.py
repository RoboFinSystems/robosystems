"""Ledger API routers."""

from fastapi import APIRouter

from .accounts import router as accounts_router
from .entity import router as entity_router
from .summary import router as summary_router
from .taxonomies import router as taxonomies_router
from .transactions import router as transactions_router
from .trial_balance import router as trial_balance_router

router = APIRouter()
router.include_router(entity_router)
router.include_router(accounts_router)
router.include_router(taxonomies_router)
router.include_router(transactions_router)
router.include_router(trial_balance_router)
router.include_router(summary_router)
