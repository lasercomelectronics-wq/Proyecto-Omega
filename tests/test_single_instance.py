from pathlib import Path

from app.single_instance import SingleInstanceError, SingleInstanceLock


def test_single_instance_acquire_and_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime" / "bot.lock"
    lock = SingleInstanceLock(lock_path)
    lock.acquire()
    assert lock_path.exists()
    lock.release()
    assert not lock_path.exists()


def test_second_acquire_blocked(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime" / "bot.lock"
    first = SingleInstanceLock(lock_path)
    second = SingleInstanceLock(lock_path)
    first.acquire()
    try:
        try:
            second.acquire()
            assert False, "expected lock to be blocked"
        except SingleInstanceError:
            pass
    finally:
        first.release()


def test_stale_lock_cleanup(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime" / "bot.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text("999999", encoding="utf-8")
    lock = SingleInstanceLock(lock_path)
    lock.acquire()
    assert lock.last_stale_pid == 999999
    lock.release()
