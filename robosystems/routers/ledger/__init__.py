"""Ledger API routers."""

from fastapi import APIRouter

from .account_rollups import router as account_rollups_router
from .accounts import router as accounts_router
from .closing_book import router as closing_book_router
from .entity import router as entity_router
from .fiscal_calendar import router as fiscal_calendar_router
from .periods import router as periods_router
from .publish_lists import router as publish_lists_router
from .reports import router as reports_router
from .schedules import router as schedules_router
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
router.include_router(reports_router)
router.include_router(schedules_router)
router.include_router(publish_lists_router)
router.include_router(account_rollups_router)
router.include_router(closing_book_router)
router.include_router(fiscal_calendar_router)
router.include_router(periods_router)
