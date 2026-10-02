from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import firestore_repo
import scraper
import sync_common

try:
    import firebase_admin
    from firebase_admin import auth as firebase_auth
except Exception:  # pragma: no cover
    firebase_admin = None
    firebase_auth = None

logger = logging.getLogger("scce.latest_api")

MAX_CONCURRENT_DATES = 2
MAX_REFRESH_DATES = max(1, int(os.getenv("MAX_REFRESH_DATES", "20")))
# This is the number of API refresh requests allowed concurrently per student.
# A successful/failed request releases the lock; the Firestore-first check makes
# subsequent calls cheap when the requested dates already exist.

app = FastAPI(title="SCCE Latest Attendance API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5500",
        "https://scceclub-5681e.web.app",
        "https://scceclub-5681e.firebaseapp.com",
    ],
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
_student_locks: dict[str, threading.Lock] = {}
_student_locks_guard = threading.Lock()
_last_refresh_attempt: dict[str, float] = {}
REFRESH_COOLDOWN_SECONDS = max(0, int(os.getenv("REFRESH_COOLDOWN_SECONDS", "60")))


class RefreshRequest(BaseModel):
    hall_ticket: str = Field(min_length=1, max_length=64)


class RefreshResponse(BaseModel):
    status: str
    hall_ticket: str
    stored_dates: list[str] = []
    fetched_dates: list[str] = []
    skipped_existing: list[str] = []
    not_offered: list[str] = []
    failed_dates: list[str] = []
    message: str


def _normalise_hall_ticket(value: str) -> str:
    return value.strip().upper()


def _student_lock(hall_ticket: str) -> threading.Lock:
    with _student_locks_guard:
        return _student_locks.setdefault(hall_ticket, threading.Lock())


def _verify_token(authorization: str | None) -> dict[str, Any] | None:
    """Verify Firebase ID token when Firebase Admin auth is available.

    Tests can omit auth. Production should set REQUIRE_FIREBASE_AUTH=true.
    """
    required = os.getenv("REQUIRE_FIREBASE_AUTH", "true").strip().lower() in {
        "1", "true", "yes", "on"
    }
    if not authorization:
        if required:
            raise HTTPException(status_code=401, detail="Missing Firebase ID token")
        return None

    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization must use Bearer token")

    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Empty Firebase ID token")

    if firebase_auth is None:
        if required:
            raise HTTPException(status_code=503, detail="Firebase Admin authentication is unavailable")
        return None

    try:
        # firestore_repo owns Firebase Admin initialisation, so reuse it
        # rather than creating a second Firebase app/client.
        firestore_repo.init_firebase()
        return firebase_auth.verify_id_token(token)
    except Exception as exc:
        logger.warning("Firebase token verification failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid Firebase ID token") from exc


def _validate_student(hall_ticket: str) -> dict:
    student = firestore_repo.get_student(hall_ticket)
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    if student.get("status") != "ACTIVE":
        raise HTTPException(status_code=403, detail="Student is not active")
    return student


def _date_is_stored(hall_ticket: str, date: str) -> bool:
    return firestore_repo.get_attendance_date(hall_ticket, date) is not None


def _scrape_one_date(hall_ticket: str, date: str) -> tuple[str, dict | None, str | None]:
    """One isolated date task. Each task owns its SCCE session."""
    session = None
    try:
        session = sync_common.authenticate(hall_ticket)
        result, session = sync_common.scrape_date_with_retry(
            session,
            hall_ticket,
            date,
            max_attempts=1,
        )
        # A second Firestore read immediately before write closes the normal
        # duplicate window when two refresh requests race after the lock is
        # released by a process restart.
        if _date_is_stored(hall_ticket, date):
            existing = firestore_repo.get_attendance_date(hall_ticket, date)
            return date, existing, "already_stored"

        firestore_repo.save_attendance_date(hall_ticket, result)
        return date, result, None
    except Exception as exc:
        return date, None, str(exc)
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


def refresh_latest(hall_ticket: str) -> dict[str, Any]:
    hall_ticket = _normalise_hall_ticket(hall_ticket)
    if not hall_ticket:
        raise ValueError("hall_ticket is required")

    _validate_student(hall_ticket)

    lock = _student_lock(hall_ticket)
    if not lock.acquire(blocking=False):
        return {
            "status": "ALREADY_PROCESSING",
            "hall_ticket": hall_ticket,
            "stored_dates": firestore_repo.list_attendance_dates(hall_ticket),
            "fetched_dates": [],
            "skipped_existing": [],
            "not_offered": [],
            "failed_dates": [],
            "message": "A refresh for this student is already running.",
        }

    try:
        now = __import__("time").monotonic()
        previous = _last_refresh_attempt.get(hall_ticket)
        if previous is not None and REFRESH_COOLDOWN_SECONDS > 0:
            elapsed = now - previous
            if elapsed < REFRESH_COOLDOWN_SECONDS:
                return {
                    "status": "RETRY_LATER",
                    "hall_ticket": hall_ticket,
                    "stored_dates": firestore_repo.list_attendance_dates(hall_ticket),
                    "fetched_dates": [],
                    "skipped_existing": [],
                    "not_offered": [],
                    "failed_dates": [],
                    "message": f"Refresh was attempted recently; try again in {int(REFRESH_COOLDOWN_SECONDS - elapsed) + 1}s.",
                }

        _last_refresh_attempt[hall_ticket] = now
        stored_before = set(firestore_repo.list_attendance_dates(hall_ticket))

        # Only SCCE's selector decides whether a calendar date is actually
        # available. We do not invent weekends/holidays locally.
        session = None
        try:
            session = sync_common.authenticate(hall_ticket)
            offered_dates = scraper.get_available_dates_from_session(session)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

        offered_dates = sorted(set(offered_dates))
        missing_or_new = [d for d in offered_dates if d not in stored_before]

        if not missing_or_new:
            return {
                "status": "ALREADY_UP_TO_DATE",
                "hall_ticket": hall_ticket,
                "stored_dates": sorted(stored_before),
                "fetched_dates": [],
                "skipped_existing": offered_dates,
                "not_offered": [],
                "failed_dates": [],
                "message": "Attendance is already up to date with SCCE's currently offered dates.",
            }

        # This is a resume-first refresh, not an unrestricted historical sync.
        # Always start at the OLDEST missing offered date. If there are many
        # missing dates, the next API press continues from where this run stopped.
        work_dates = missing_or_new[:MAX_REFRESH_DATES]

        fetched: list[str] = []
        failed: list[str] = []
        skipped: list[str] = []

        # IMPORTANT: max_workers is hard-capped at 2. Dates in the same batch
        # execute simultaneously; the next batch starts only after the first
        # batch has completed.
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DATES) as pool:
            future_map = {
                pool.submit(_scrape_one_date, hall_ticket, date): date
                for date in work_dates
            }

            for future in as_completed(future_map):
                date = future_map[future]
                try:
                    returned_date, result, error = future.result()
                except Exception as exc:  # defensive boundary
                    returned_date, result, error = date, None, str(exc)

                if error is None:
                    fetched.append(returned_date)
                elif error == "already_stored":
                    skipped.append(returned_date)
                elif sync_common._is_date_not_available(error):
                    # This should rarely happen because offered_dates came from
                    # the selector, but another selector refresh may disagree.
                    failed.append(returned_date)
                else:
                    failed.append(returned_date)
                    logger.error("LATEST_REFRESH_DATE_FAILED hall=%s date=%s error=%s", hall_ticket, returned_date, error)

        stored_after = sorted(set(firestore_repo.list_attendance_dates(hall_ticket)))
        fetched.sort()
        failed.sort()
        skipped.sort()

        if failed and fetched:
            status = "PARTIAL"
            message = "Some latest attendance dates were saved; some dates failed."
        elif failed:
            status = "FAILED"
            message = "Latest attendance refresh failed for all requested dates."
        elif fetched or skipped:
            status = "SUCCESS"
            message = "Latest attendance refresh completed."
        else:
            status = "NO_NEW_ATTENDANCE"
            message = "No new attendance was available to save."

        return {
            "status": status,
            "hall_ticket": hall_ticket,
            "stored_dates": stored_after,
            "fetched_dates": fetched,
            "skipped_existing": skipped,
            "not_offered": [],
            "failed_dates": failed,
            "message": message,
        }
    finally:
        lock.release()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "scce-latest-attendance"}


@app.post("/attendance/latest", response_model=RefreshResponse)
def latest_attendance(
    request: RefreshRequest,
    authorization: str | None = Header(default=None),
):
    _verify_token(authorization)
    try:
        return refresh_latest(request.hall_ticket)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Latest attendance API failed")
        raise HTTPException(status_code=500, detail="Attendance refresh failed") from exc
