import sys
import types
from unittest.mock import patch

# Keep these unit tests runnable without Firebase credentials or the production
# project modules. The real deployment imports the user's existing modules.
fake_repo = types.ModuleType("firestore_repo")
fake_repo.get_student = lambda hall: {"hallTicket": hall, "status": "ACTIVE"}
fake_repo.list_attendance_dates = lambda hall: []
fake_repo.get_attendance_date = lambda hall, date: None
fake_repo.save_attendance_date = lambda hall, result: None
fake_repo.init_firebase = lambda: None

fake_scraper = types.ModuleType("scraper")
fake_scraper.get_available_dates_from_session = lambda session: []

fake_sync = types.ModuleType("sync_common")
fake_sync.authenticate = lambda hall: object()
fake_sync.scrape_date_with_retry = lambda session, hall, date, max_attempts=None: ({"date": date}, session)
fake_sync._is_date_not_available = lambda message: False

sys.modules.setdefault("firestore_repo", fake_repo)
sys.modules.setdefault("scraper", fake_scraper)
sys.modules.setdefault("sync_common", fake_sync)

firebase_admin = types.ModuleType("firebase_admin")
firebase_auth = types.ModuleType("firebase_admin.auth")
firebase_auth.verify_id_token = lambda token: {"uid": "test"}
firebase_admin.auth = firebase_auth
sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.auth", firebase_auth)

import main


def base_student():
    return {"hallTicket": "23N01A05A9", "status": "ACTIVE"}


def reset_state():
    main._last_refresh_attempt.clear()
    main._student_locks.clear()


def test_already_up_to_date():
    reset_state()
    with patch.object(main.firestore_repo, "get_student", return_value=base_student()), \
         patch.object(main.firestore_repo, "list_attendance_dates", return_value=["2026-09-25", "2026-09-26"]), \
         patch.object(main.sync_common, "authenticate", return_value=object()), \
         patch.object(main.scraper, "get_available_dates_from_session", return_value=["2026-09-25", "2026-09-26"]):
        result = main.refresh_latest("23n01a05a9")

    assert result["status"] == "ALREADY_UP_TO_DATE"
    assert result["fetched_dates"] == []


def test_fetches_missing_dates_from_oldest_gap():
    reset_state()
    stored = {"2026-09-25"}
    calls = []

    def fake_list(_):
        return sorted(stored)

    def fake_scrape(hall, date):
        calls.append(date)
        stored.add(date)
        return date, {"date": date}, None

    with patch.object(main.firestore_repo, "get_student", return_value=base_student()), \
         patch.object(main.firestore_repo, "list_attendance_dates", side_effect=fake_list), \
         patch.object(main.sync_common, "authenticate", return_value=object()), \
         patch.object(main.scraper, "get_available_dates_from_session", return_value=[
             "2026-09-25", "2026-09-26", "2026-09-27"
         ]), \
         patch.object(main, "_scrape_one_date", side_effect=fake_scrape):
        result = main.refresh_latest("23N01A05A9")

    assert result["status"] == "SUCCESS"
    assert set(calls) == {"2026-09-26", "2026-09-27"}


def test_max_two_dates_at_once():
    reset_state()
    import threading
    import time

    active = 0
    peak = 0
    guard = threading.Lock()

    def fake_scrape(hall, date):
        nonlocal active, peak
        with guard:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with guard:
            active -= 1
        return date, {"date": date}, None

    with patch.object(main.firestore_repo, "get_student", return_value=base_student()), \
         patch.object(main.firestore_repo, "list_attendance_dates", return_value=[]), \
         patch.object(main.sync_common, "authenticate", return_value=object()), \
         patch.object(main.scraper, "get_available_dates_from_session", return_value=[
             "2026-09-25", "2026-09-26", "2026-09-27", "2026-09-28"
         ]), \
         patch.object(main, "_scrape_one_date", side_effect=fake_scrape):
        result = main.refresh_latest("23N01A05A9")

    assert result["status"] == "SUCCESS"
    assert peak == 2


def test_second_refresh_is_blocked_by_cooldown():
    reset_state()
    with patch.object(main.firestore_repo, "get_student", return_value=base_student()), \
         patch.object(main.firestore_repo, "list_attendance_dates", return_value=[]), \
         patch.object(main.sync_common, "authenticate", return_value=object()), \
         patch.object(main.scraper, "get_available_dates_from_session", return_value=[]):
        first = main.refresh_latest("23N01A05A9")
        second = main.refresh_latest("23N01A05A9")

    assert first["status"] == "ALREADY_UP_TO_DATE"
    assert second["status"] == "RETRY_LATER"


def test_student_lock_prevents_duplicate_refresh():
    reset_state()
    lock = main._student_lock("LOCKTEST")
    assert lock.acquire(blocking=False)
    try:
        with patch.object(main.firestore_repo, "get_student", return_value=base_student()), \
             patch.object(main.firestore_repo, "list_attendance_dates", return_value=[]):
            result = main.refresh_latest("LOCKTEST")
        assert result["status"] == "ALREADY_PROCESSING"
    finally:
        lock.release()


def test_scrape_attempt_limit_is_one():
    reset_state()
    with patch.object(main.sync_common, "authenticate", return_value=object()), \
         patch.object(main.sync_common, "scrape_date_with_retry", return_value=({"date": "2026-09-26"}, object())) as scrape, \
         patch.object(main.firestore_repo, "get_attendance_date", return_value=None), \
         patch.object(main.firestore_repo, "save_attendance_date"):
        main._scrape_one_date("23N01A05A9", "2026-09-26")

    assert scrape.call_args.kwargs["max_attempts"] == 1
